/**
$(D TempAlloc) is a memory allocator based on a thread-local segmented stack.
A segmented stack is similar to a regular stack in that memory is allocated
and freed in last in, first out order.  When memory is requested from a
segmented stack, it first checks whether enough space is available in the
current segment, and if so increments the stack pointer and returns.  If not,
a new segment is allocated.  When memory is freed, the stack pointer is
decremented.  If the last segment is no longer in use, it may be returned to
where it was allocated from or retained for future use.

$(D TempAlloc) has the following advantages compared to allocation on the
call stack:

1.  Pointers to memory allocated on the $(D TempAlloc) stack are still
    valid when the function they were allocated from returns.
    Functions can be written to create and return data structures on the
    $(D TempAlloc) stack.

2.  Since it is a segmented stack, large allocations can be performed with no
    danger of stack overflow errors.

3.  It guarantees 16-byte alignment of all allocated objects.

It has thet following disadvantages:

1.  The extra checks due to stack segmentation make allocation slightly
    slower and prevent simultanelous deallocation of several objects from
    being a single decrement of the stack pointer.

2.  Memory allocated on TempAlloc is accessed through an extra layer of
    pointer indirection compared to memory on the call stack.

It has the following advantages compared to heap allocation:

1.  Both allocation and deallocation are extremely fast.  Most allocations
    consist of verifying enough space is available, incrementing a pointer and
    a performing a few cheap bookkeeping operations.  Most deallocations
    consist decrementing a pointer and performing a few cheap bookkeeping
    operations.

2.  The segmented stack is thread-local, so synchronization is only needed
    when a segment needs to be allocated or freed.

3.  Fragmentation is not an issue when allocating memory on the
    $(D TempAlloc) stack, though it can be an issue when trying to allocate
    a new segment.

It has the following disadvantages compared to heap allocation:

1.  The requirement that memory be freed in last in, first out order.

2.  No automatic garbage collection.

Note:

The first segment of the $(D TempAlloc) stack is allocated lazily, so
no space is allocated in any thread that does not use $(D TempAlloc).

Synopsis:
---
// Initialize a new TempAlloc frame.
TempAlloc.frameInit();

// Allocate a temporary array on the TempAlloc stack.
auto arr = TempAlloc.newArray!(double[][][])(8, 6, 7);
assert(arr.length == 8);
assert(arr[0].length == 6);
assert(arr[0][0].length == 7);

// Release all memory allocated on TempAlloc since the last call to
// TempAlloc.frameInit().
TempAlloc.frameFree();
---

Author:  David Simcha
Copyright:  Copyright (c) 2008-2011, David Simcha.
License:    $(WEB boost.org/LICENSE_1_0.txt, Boost License 1.0)
*/

module std.tempalloc;

import std.traits, core.memory, std.range, core.exception, std.conv,
    std.algorithm, std.typetuple;

static import core.stdc.stdlib;

// This is just for convenience/code readability/saving typing.
private enum ptrSize = (void*).sizeof;

// This was accidentally assumed in a few places and I'm too lazy to fix it
// until I see proof that it needs to be fixed.
static assert(bool.sizeof == 1);

// Memory allocation routines.  These wrap malloc(), free() and realloc(),
// and guarantee alignment.
private enum size_t alignBytes = 16;

/**
This struct functions as a namespace for all of the core TempAlloc
functions.  All of its member functions are static.
*/
struct TempAlloc {
private:
    static struct Stack(T) {  // Simple, fast stack w/o error checking.
        private size_t capacity;
        private size_t index;
        private T* data;
        private enum sz = T.sizeof;

        private static size_t max(size_t lhs, size_t rhs) pure {
            return (rhs > lhs) ? rhs : lhs;
        }

        void push(T elem) {
            if (capacity == index) {
                capacity = max(16, capacity * 2);
                data = cast(T*) core.stdc.stdlib.realloc(data, capacity * sz);
            }
            data[index++] = elem;
        }

        T pop() {
            index--;
            auto ret = data[index];
            data[index] = T.init;  // Prevent false ptrs.
            return ret;
        }

        void destroy() {
            if(data) {
                core.stdc.stdlib.free(data);
                data = null;
            }
        }
    }

    struct Block {
        size_t used = 0;
        void* space = null;
    }

    final class State {
        size_t used;
        void* space;
        size_t totalAllocs;
        void*[] lastAlloc;
        uint nblocks;
        uint nfree;
        size_t frameIndex;

        // inUse holds info for all blocks except the one currently being
        // allocated from.  freelist holds space ptrs for all free blocks.
        Stack!(Block) inUse;
        Stack!(void*) freelist;

        void putLast(void* last) {
            // Add an element to lastAlloc, checking length first.
            if (totalAllocs == lastAlloc.length)
                doubleSize(lastAlloc);
            lastAlloc[totalAllocs] = cast(void*) last;
            totalAllocs++;
        }

        void destroy() {
            if(space) {
                alignedFree(space);
                space = null;
            }

            if(lastAlloc) {
                core.stdc.stdlib.free(lastAlloc.ptr);
                lastAlloc = null;
            }

            while(inUse.index > 0) {
                auto toFree = inUse.pop();
                alignedFree(toFree.space);
            }

            inUse.destroy();

            while(freelist.index > 0) {
                auto toFree = freelist.pop();
                alignedFree(toFree);
            }

            freelist.destroy();
        }

        ~this() {
            destroy();
        }
    }

    static ~this() {
        if(state) {
            state.destroy();
            state = null;
        }
    }

    enum size_t blockSize = 4 * 1024 * 1024;
    enum size_t nBookKeep = blockSize / alignBytes;
    static State state;

    static void doubleSize(ref void*[] lastAlloc) {
        size_t newSize = lastAlloc.length * 2;
        void** ptr = cast(void**) core.stdc.stdlib.realloc(
            lastAlloc.ptr, newSize * ptrSize);
        lastAlloc = ptr[0..newSize];
    }

    static State stateInit() {
        State stateCopy;
        try { stateCopy = new State; } catch { outOfMemory(); }

        with(stateCopy) {
            space = alignedMalloc(blockSize);

            // We don't need 16-byte alignment for the bookkeeping array.
            lastAlloc = (cast(void**) core.stdc.stdlib.malloc(nBookKeep))
                        [0..nBookKeep / ptrSize];
            nblocks++;
        }

        state = stateCopy;
        return stateCopy;
    }

    // CTFE function, for static assertions.  Can't use bsr/bsf b/c it has
    // to be usable at compile time.
    static bool isPowerOf2(size_t num) pure {
        int nBitsSet;
        foreach(shift; 0..size_t.sizeof * 8) {
            size_t mask = (cast(size_t) 1) << shift;
            nBitsSet += ((num & mask) > 0);
        }

        return nBitsSet == 1;
    }

    static size_t getAligned(size_t nbytes) pure {
        static assert(isPowerOf2(alignBytes));
        return (nbytes + (alignBytes - 1)) & (~(alignBytes - 1));
    }

    static State getState() {
        State stateCopy = state;
        return (stateCopy is null) ? stateInit : stateCopy;
    }

    static State frameInit(State stateCopy) {
        with(stateCopy) {
            putLast( cast(void*) frameIndex );
            frameIndex = totalAllocs;
        }
        return stateCopy;
    }

    static void frameFree(State stateCopy) {
        with(stateCopy) {
            while (totalAllocs > frameIndex) {
                free(stateCopy);
            }
            frameIndex = cast(size_t) lastAlloc[--totalAllocs];
        }
    }

    static void* malloc(size_t nbytes, State stateCopy) {
        nbytes = getAligned(nbytes);
        with(stateCopy) {
            void* ret;
            if (blockSize - used >= nbytes) {
                ret = space + used;
                used += nbytes;
            } else if (nbytes > blockSize) {
                ret = alignedMalloc(nbytes);
            } else if (nfree > 0) {
                inUse.push(Block(used, space));
                space = freelist.pop;
                used = nbytes;
                nfree--;
                nblocks++;
                ret = space;
            } else { // Allocate more space.
                inUse.push(Block(used, space));
                space = alignedMalloc(blockSize);
                nblocks++;
                used = nbytes;
                ret = space;
            }
            putLast(ret);
            return ret;
        }
    }

    static void free(State stateCopy) {
        with(stateCopy) {
            void* lastPos = lastAlloc[--totalAllocs];

            // Handle large blocks.
            if (lastPos > space + blockSize || lastPos < space) {
                alignedFree(lastPos);
                return;
            }

            used = (cast(size_t) lastPos) - (cast(size_t) space);
            if (nblocks > 1 && used == 0) {
                freelist.push(space);
                Block newHead = inUse.pop;
                space = newHead.space;
                used = newHead.used;
                nblocks--;
                nfree++;

                if (nfree >= nblocks * 2) {
                    foreach(i; 0..nfree / 2) {
                        alignedFree(freelist.pop);
                        nfree--;
                    }
                }
            }
        }
    }

public:

    /**
    Pushes the current stack position onto an internal bookkeeping stack.
    A call to $(D frameFree) frees all memory allocated since the last call
    to $(D frameInit).
    */
    static void frameInit() {
        frameInit(getState());
    }

    /**
    Frees all memory allocated by $(D TempAlloc) since the last call to
    $(D frameInit).  This can be used to free all $(D TempAlloc) memory
    allocated within a function upon returning from that function.

    Examples:
    ---
    void useFrameInit() {
        // Mark the current position.
        TempAlloc.frameInit();

        // Free all memory allocated by this function on exit.
        scope(exit) TempAlloc.frameFree();

        // All of these will be freed when useFrameInit() exits.
        auto ptr1 = TempAlloc.malloc(3);
        auto ptr2 = TempAlloc.malloc(1);
        auto ptr3 = TempAlloc.malloc(4);
    }
    ---
    */
    static void frameFree() {
        frameFree(getState());
    }

    /**
    Allocates $(D nbytes) bytes on the $(D TempAlloc) stack.  The last
    block allocated in the current thread can be freed by calling
    $(D TempAlloc.free).  The memory returned by this function is not scanned
    for pointers by the garbage collector unless $(D GC.addRange) is called.
    */
    static void* malloc(size_t nbytes) {
        return malloc(nbytes, getState());
    }

    /**Allocates an array of type $(D T) on the $(D TempAlloc) stack.
    For performance reasons, the returned array is not initialized.  It
    is not scanned for pointers by the garbage collector unless $(D GC.addRange)
    is called.   $(D T) may be a multidimensional array.  In this case sizes
    may be specified for any number of dimensions from 1 to the number in $(D T).

    Examples:
    ---
    double[] arr = TempAlloc.newArray!(double[])(100);
    assert(arr.length == 100);

    double[][] matrix = TempAlloc.newArray!(double[][])(42, 31);
    assert(matrix.length == 42);
    assert(matrix[0].length == 31);
    ---
    */
    static auto newArray(T, I...)(I sizes)
    if(allSatisfy!(isIntegral, I)) {
        static assert(sizes.length >= 1,
            "Cannot allocate an array without the size of at least the first " ~
            " dimension.");
        static assert(sizes.length <= nDimensions!T,
            to!string(sizes.length) ~ " dimensions specified for a " ~
            to!string(nDimensions!T) ~ " dimensional array.");

        alias typeof(T.init[0]) E;

        auto ptr = cast(E*) TempAlloc.malloc(sizes[0] * E.sizeof);
        auto ret = ptr[0..sizes[0]];

        static if(sizes.length > 1) {
            foreach(ref elem; ret) {
                elem = uninitializedArray!(E)(sizes[1..$]);
            }
        }

        return ret;
    }

    unittest {
        double[] arr = newArray!(double[])(100);
        assert(arr.length == 100);

        double[][] matrix = newArray!(double[][])(42, 31);
        assert(matrix.length == 42);
        assert(matrix[0].length == 31);
    }

    /**
    Frees the last block of memory allocated in the current thread by
    $(D TempAlloc).  Since all memory must be allocated and freed in last in
    first out order within a thread, there's no need to pass a pointer to the
    block being freed.  This bookkeeping is handled internally.
    */
    static void free() {
        free(getState());
    }

    /**
    Returns the maximum number of bytes that may be allocated in the
    current stack segment.
    */
    static size_t segmentSlack() @property {
        return blockSize - getState().used;
    }

}


/**
Concatenates any number of arrays of the same type, placing results on
the TempAlloc stack.  The returned array is not scanned for pointers
by the garbage collector unless $(D GC.addRange) is called.

Examples:
---
auto a = [1, 2, 3];
auto b = [4, 5, 6];
auto c = [7, 8, 9];

auto d = stackCat(a, b, c);
assert(d == [1, 2, 3, 4, 5, 6, 7, 8, 9]);
---
*/
CommonType!(staticMap!(ElementType, T))[] stackCat(T...)(T data)
if(allSatisfy!(isArray, T)) {
    size_t totalLen = 0;
    foreach(array; data) {
        totalLen += array.length;
    }

    alias ElementType!(typeof(return)) E;
    auto ret = TempAlloc.newArray!(Unqual!E[])(totalLen);

    size_t offset = 0;
    foreach(array; data) {
        foreach(elem; array) {
            ret[offset] = elem;
            offset++;
        }
    }
    return cast(E[]) ret;
}

unittest {
    mixin(newFrame);
    auto a = [1, 2, 3];
    auto b = [4, 5, 6];
    auto c = [7, 8, 9];

    auto d = stackCat(a, b, c);
    assert(d == [1, 2, 3, 4, 5, 6, 7, 8, 9]);

    double[] foo = [1.0, 2, 3];
    float[] bar = [4f, 5f, 6f];
    double[] foobar = stackCat(foo, bar);
    assert(foobar == [1.0, 2, 3, 4, 5, 6]);
}

/**
Copies $(D range) to an array.  The array will be located on the
$(D TempAlloc) stack and not scanned for pointers by the garbage collector
under either of the following conditions:

1.  $(D std.traits.hasIndirections!(ElementType!R)) is false, or

2.  $(D R) is a builtin array.  In this case $(D range) maintains pointers
    to all elements at least until $(D tempArray) returns, preventing the
    elements from being freed by the garbage collector.  A similar assumption
    cannot be made for ranges other than builtin arrays.

If neither condition is met, the array is returned on the C heap
and $(D GC.addRange) is called.  In either case, $(D TempAlloc.free) or
$(D TempAlloc.frameFree) will free the array as if it had been allocated on
the $(D TempAlloc) stack.

Rationale:  The most common reason to call $(D tempArray) on an array is to
            modify its contents inside a function without affecting the
            caller's view.  In this case $(D range) is not modified and
            prevents the elements from being freed by the garbage
            collector.

Examples:
---
auto arr = tempArray(iota(5));
assert(arr == [0, 1, 2, 3, 4]);
---
 */
Unqual!(ElementType!(R))[] tempArray(R)(R range)
if(isInputRange!(R) && (isArray!(R) || !hasIndirections!(ElementType!(R)))) {
    alias ElementType!(R) E;
    alias Unqual!(E) U;
    static if(hasLength!(R)) {
        U[] ret = TempAlloc.newArray!(U[])(range.length);
        copy(range, ret);
        return ret;
    } else {
        auto state = TempAlloc.getState();
        auto startPtr = TempAlloc.malloc(0);
        size_t bytesCopied = 0;

        while(!range.empty) {  // Make sure range interface is being used.
            auto elem = range.front;
            if(state.used + U.sizeof <= TempAlloc.blockSize) {
                range.popFront;
                *(cast(U*) (startPtr + bytesCopied)) = elem;
                bytesCopied += U.sizeof;
                state.used += U.sizeof;
            } else {
                if(bytesCopied + U.sizeof >= TempAlloc.blockSize / 2) {
                    // Then just heap-allocate.
                    U[] result = (cast(U*) alignedMalloc(bytesCopied * 2))
                        [0..bytesCopied / U.sizeof * 2];

                    immutable elemsCopied = bytesCopied / U.sizeof;
                    result[0..elemsCopied] = (cast(U*) startPtr)[0..elemsCopied];
                    finishCopy(result, range, elemsCopied);
                    TempAlloc.free;
                    state.putLast(result.ptr);
                    return result;
                } else {
                    U[] oldData = (cast(U*) startPtr)[0..bytesCopied / U.sizeof];
                    state.used -= bytesCopied;
                    state.totalAllocs--;
                    U[] newArray = TempAlloc.newArray!(U[])(bytesCopied / U.sizeof + 1);
                    newArray[0..oldData.length] = oldData[];
                    startPtr = state.space;
                    newArray[$ - 1] = elem;
                    bytesCopied += U.sizeof;
                    range.popFront;
                }
            }
        }
        auto rem = bytesCopied % alignBytes;
        if(rem != 0) {
            auto toAdd = 16 - rem;
            if(state.used + toAdd < TempAlloc.blockSize) {
                state.used += toAdd;
            } else {
                state.used = TempAlloc.blockSize;
            }
        }
        return (cast(U*) startPtr)[0..bytesCopied / U.sizeof];
    }
}

// Ditto but not worth its own ddoc.
Unqual!(ElementType!(R))[] tempArray(R)(R range)
if(isInputRange!(R) && !(isArray!(R) || !hasIndirections!(ElementType!(R)))) {
    // Initial guess of how much space to allocate.  It's relatively large b/c
    // the object will be short lived, so speed is more important than space
    // efficiency.
    enum initialGuess = 128;

    alias Unqual!(ElementType!R) E;
    auto arr = (cast(E*) alignedMalloc(E.sizeof * initialGuess, true))
        [0..initialGuess];

    finishCopy(arr, range, 0);
    TempAlloc.getState().putLast(arr.ptr);
    return arr;
}

// Finishes copying a range to a C heap allocated array.  Assumes the first
// half of the input array is stuff already copied and the second half is
// free space.
private void finishCopy(T, U)(ref T[] result, U range, size_t alreadyCopied) {
    void doRealloc() {
        auto newPtr = cast(T*) alignedRealloc(
            result.ptr, result.length * T.sizeof * 2, result.length * T.sizeof
        );
        result = newPtr[0..result.length * 2];
    }

    auto index = alreadyCopied;
    foreach(elem; range) {
        if(index == result.length) doRealloc();
        result[index++] = elem;
    }

    result = result[0..index];
}

unittest {
    auto arr = tempArray(iota(5));
    assert(arr == [0, 1, 2, 3, 4]);

    // Create quick and dirty finite but lengthless range.
    static struct Count {
        uint num;
        uint upTo;
        @property size_t front() {
            return num;
        }
        void popFront() {
            num++;
        }
        @property bool empty() {
            return num >= upTo;
        }
    }

    TempAlloc.malloc(1024 * 1024 * 3);
    Count count;
    count.upTo = 1024 * 1025;
    auto asArray = tempArray(count);
    foreach(i, elem; asArray) {
        assert(i == elem, to!(string)(i) ~ "\t" ~ to!(string)(elem));
    }
    assert(asArray.length == 1024 * 1025);
    TempAlloc.free;
    TempAlloc.free;
    while(TempAlloc.getState().freelist.index > 0) {
        alignedFree(TempAlloc.getState().freelist.pop);
    }
}

/**
A convenience mixin, equivalent to:

---
TempAlloc.frameInit();  scope(exit) TempAlloc.frameFree();
---

Example:
    ---
void* useNewFrame() {
    // This will not be freed when useNewFrame() exits.
    auto ptr1 = TempAlloc.malloc(8);

    mixin(newFrame);

    // All of these will be freed when useNewFrame() exits.
    auto ptr2 = TempAlloc.malloc(3);
    auto ptr3 = TempAlloc.malloc(1);
    auto ptr4 = TempAlloc.malloc(4);

    // ptr1 is still valid after useNewFrame() exits.
    return ptr1;
}
---
*/
immutable string newFrame =
    "TempAlloc.frameInit(); scope(exit) TempAlloc.frameFree();";

unittest {
    /* Not a particularly good unittest in that it depends on knowing the
     * internals of TempAlloc, but it's the best I could come up w/.  This
     * is really more of a stress test/sanity check than a normal unittest.*/

    // Make sure state is completely reset.
    if(TempAlloc.state) TempAlloc.state.destroy();
    TempAlloc.state = null;

     // First test to make sure a large number of allocations does what it's
     // supposed to in terms of reallocing lastAlloc[], etc.
     enum nIter =  TempAlloc.blockSize * 5 / alignBytes;
     foreach(i; 0..nIter) {
         TempAlloc.malloc(alignBytes);
     }
     assert(TempAlloc.getState().nblocks == 5, to!string(TempAlloc.getState().nblocks));
     assert(TempAlloc.getState().nfree == 0);
     foreach(i; 0..nIter) {
        TempAlloc.free;
    }
    assert(TempAlloc.getState().nblocks == 1);
    assert(TempAlloc.getState().nfree == 2);

    // Make sure logic for freeing excess blocks works.  If it doesn't this
    // test will run out of memory.
    enum allocSize = TempAlloc.blockSize / 2;
    foreach(i; 0..50) {
        foreach(j; 0..50) {
            TempAlloc.malloc(allocSize);
        }
        foreach(j; 0..50) {
            TempAlloc.free;
        }
    }

    // Make sure data is stored properly.
    foreach(i; 0..10) {
        TempAlloc.malloc(allocSize);
    }
    foreach(i; 0..5) {
        TempAlloc.free;
    }
    void* space = TempAlloc.state.space;
    size_t used = TempAlloc.state.used;

    TempAlloc.frameInit;
    // This array of arrays should not be scanned by the GC because otherwise
    // bugs caused th not having the GC scan certain internal things in
    // TempAlloc that it should would not be exposed.
    uint[][] arrays = (cast(uint[]*) GC.malloc((uint[]).sizeof * 10,
                       GC.BlkAttr.NO_SCAN))[0..10];
    foreach(i; 0..10) {
        uint[] data = TempAlloc.newArray!(uint[])(250_000);
        foreach(j, ref e; data) {
            e = cast(uint) (j * (i + 1));  // Arbitrary values that can be read back later.
        }
        arrays[i] = data;
    }

    // Make stuff get overwrriten if blocks are getting GC'd when they're not
    // supposed to.
    GC.minimize;  // Free up all excess pools.
    uint[][] foo;
    foreach(i; 0..40) {
        foo ~= new uint[1_048_576];
    }
    foo = null;

    for(size_t i = 9; i != size_t.max; i--) {
        foreach(j, e; arrays[i]) {
            assert(e == j * (i + 1));
        }
    }
    TempAlloc.frameFree;
    assert(space == TempAlloc.state.space);
    assert(used == TempAlloc.state.used);
    while(TempAlloc.state.nblocks > 1 || TempAlloc.state.used > 0) {
        TempAlloc.free;
    }

    // Test that everything is really getting destroyed properly when
    // destroy() is called.  If not then this test will run out of memory.
    foreach(i; 0..1000) {
        TempAlloc.state.destroy();
        TempAlloc.state = null;

        foreach(j; 0..1_000) {
            auto ptr = TempAlloc.malloc(20_000);
            assert((cast(size_t) ptr) % alignBytes == 0);
        }

        foreach(j; 0..500) {
            TempAlloc.free();
        }
    }
}

private  void outOfMemory()  {
    throw new OutOfMemoryError("Out of memory in TempAlloc.");
}

private void* alignedMalloc(size_t size, bool shouldAddRange = false) {
    // We need (alignBytes - 1) extra bytes to guarantee alignment, 1 byte
    // to store the shouldAddRange flag, and ptrSize bytes to store
    // the pointer to the beginning of the block.
    void* toFree = core.stdc.stdlib.malloc(
        alignBytes + ptrSize + size
    );

    if(toFree is null) outOfMemory();

    // Add the offset for the flag and the base pointer.
    auto intPtr = cast(size_t) toFree + ptrSize + 1;

    // Align it.
    intPtr = (intPtr + alignBytes - 1) & (~(alignBytes - 1));
    auto ret = cast(void**) intPtr;

    // Store base pointer.
    (cast(void**) ret)[-1] = toFree;

    // Store flag.
    (cast(bool*) ret)[-1 - ptrSize] = shouldAddRange;

    if(shouldAddRange) {
        GC.addRange(ret, size);
    }

    return ret;
}

private void alignedFree(void* ptr) {
    // If it was allocated with alignedMalloc() then the pointer to the
    // beginning is at ptr[-1].
    auto addedRange = (cast(bool*) ptr)[-1 - ptrSize];

    if(addedRange) {
        GC.removeRange(ptr);
    }

    core.stdc.stdlib.free( (cast(void**) ptr)[-1]);
}

// This is used by TempAlloc, but I'm not sure enough that its interface
// isn't going to change to make it public and document it.
private void* alignedRealloc(void* ptr, size_t newLen, size_t oldLen) {
    auto storedRange = (cast(bool*) ptr)[-1 - ptrSize];
    auto newPtr = alignedMalloc(newLen, storedRange);
    memcpy(newPtr, ptr, oldLen);

    alignedFree(ptr);
    return newPtr;
}
void main() {}
