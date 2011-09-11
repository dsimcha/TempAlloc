/**
This module contains a struct that wraps D's builtin garbage collected
allocator in an allocator interface as described in 
$(D std.allocators.allocator).

Author:  David Simcha
License: $(WEB boost.org/LICENSE_1_0.txt, Boost License 1.0).
*/
module std.allocators.gc;

import core.memory;

import std.allocators.allocator;
import std.array;
import std.range;
import std.traits;
import std.typetuple;

private enum PAGESIZE = 4_096;

/**
This struct provides an interface to D's garbage collector using the standard
allocator interface.
*/
struct GCAllocator
{
    /**
    This variable controls whether memory allocated via $(D allocate)
    may contain pointers and should be scanned by the garbage collector.
    This does not affect memory allocated by $(D create), $(D newArray), 
    $(D array) or $(D uninitializedArray) since these functions have type 
    information available.  The default is $(D true).
    */
    bool scanForPointers = true;

    /**
    Forwards to $(D core.memory.GC.malloc) and sets the
    $(D GC.BlkAttr.NO_SCAN) bit if $(D scanForPointers) is false.
    */
    void* allocate(size_t nBytes)
    {
        auto flags = (scanForPointers) ?
                     cast(GC.BlkAttr) 0 : GC.BlkAttr.NO_SCAN;
        return GC.malloc(nBytes, flags);
    }

    /**
    Forwards to $(D core.memory.GC.free).
    */
    void free(void* ptr)
    {
        GC.free(ptr);
    }

    /**
    Forwards to $(D core.memory.GC.extend).
    */
    static bool resize(void* ptr, size_t newSize)
    {
        immutable bytesNeeded = newSize - GC.sizeOf(ptr);
        immutable result = GC.extend(ptr, bytesNeeded, bytesNeeded);
        return result >= newSize;
    }

    /**
    False because the GC doesn't do any checking to ensure that free'd
    pointers are valid.
    */
    enum freeIsChecked = false;

    /**
    The GC aligns allocations of <16 bytes to 16 bytes, allocations between
    16 bytes and the page size to the next larger power of two, and
    allocations greater than a page size to page boundaries.
    */
    static size_t alignBytes(size_t s)
    {
        if(s <= 16) return 16;
        if(s >= PAGESIZE) return PAGESIZE;

        for(size_t powerOf2 = 32; powerOf2 < PAGESIZE; powerOf2 <<= 1)
        {
            if(powerOf2 > s) return powerOf2;
        }

        assert(0);
    }

    /**
    The GC rounds allocations of less than 16 bytes to 16 bytes, allocations
    of between 16 bytes and a page to the next power of two, and allocations
    greater than a page to the nearest page.
    */
    static size_t allocSize(size_t s)
    {
        if(s < 16) return 16;
        if(s >= PAGESIZE) return nextPage(s);
        return nextPowerOfTwo(s);
    }

    /**
    True because this is just a wrapper around the GC and gets automatically
    reclaimed.
    */
    enum isAutomatic = true;

    /**
    False because GC allocated memory is not freed deterministically when
    this allocator goes out of scope.
    */
    enum isScoped = false;
    
    /**
    Forwards to the $(D new) operator for classes.
    */
    C create(C, Args...)(auto ref Args args) if(is(C == class))
    {
        return new C(args);
    }
    
    /**
    Forwards to the $(D new) operator for non-class objects.
    */
    T* create(T, Args...)(auto ref Args args) if(!is(T == class))
    {
        static if(is(T == struct))
        {
            static if(is(typeof(new T(args))))
            {
                return new T(args);
            }
            else
            {
                // Struct literals.
                auto ret = new T;
                *ret = T(args);
                return ret;
            }
        }
        else
        {
            static assert(args.length < 2, 
                "Cannot use more than 1 c'tor argument for a non-struct/class " 
                ~ "object."
            );
            
            auto ret = new T;
            
            static if(args.length)
            {
                *ret = args[0];
            }
            
            return ret;
        }
    }

    /**
    Forwards to the $(D new) operator for arrays.

    ---
    auto alloc = GCAllocator.init;

    // The following are equivalent:
    auto arr = new int[][](42, 86);
    auto arr = alloc.newArray!(int[][])(42, 86);
    ---
    */
    static T newArray(T, I...)(I sizes)
    if(isArray!T && allSatisfy!(isIntegral, I))
    {
        static assert(sizes.length >= 1,
                      "Cannot allocate an array without the size of at least the first " ~
                      " dimension.");
        static assert(sizes.length <= nDimensions!T,
                      to!string(sizes.length) ~ " dimensions specified for a " ~
                      to!string(nDimensions!T) ~ " dimensional array.");

        return new T(sizes);
    }

    /**
    Same as $(D newArray) except skips initialization of elements.
    */
    static T uninitializedArray(T, I...)(I sizes)
    if(isArray!T && allSatisfy!(isIntegral, I))
    {
        static assert(sizes.length >= 1,
                      "Cannot allocate an array without the size of at least the first " ~
                      " dimension.");
        static assert(sizes.length <= nDimensions!T,
                      to!string(sizes.length) ~ " dimensions specified for a " ~
                      to!string(nDimensions!T) ~ " dimensional array.");

        return std.array.uninitializedArray!T(sizes);
    }

    /**
    Forwards to $(D std.array.array).
    */
    static auto array(R)(R range) if(isInputRange!R && !isInfinite!R)
    {
        return std.array.array(range);
    }
    
    /**
    Returns a $(D std.allocators.allocator.DynamicAllocator) using $(D this) 
    as the underlying allocator.  The $(D DynamicAllocator) is allocated on the 
    GC heap.
    */
    @property DynamicAllocator dynamicAllocator()
    {
        return new DynamicAllocatorTemplate!(typeof(this))(this);
    }
}

pure nothrow @safe:

private size_t nextPowerOfTwo(size_t x)
{
    enum size_t one = 1;  // Make sure it's interpreted as a size_t.

    // Not worth doing any fancy bit twiddling hacks here.
    foreach(i; 0..64)
    {
        if(x >= (one << i)) return (one << i);
    }

    assert(0);
}

private size_t nextPage(size_t x)
{
    if(x % PAGESIZE == 0) return x;
    return ((x / PAGESIZE) + 1) * PAGESIZE;
}

unittest
{
//    static assert(isAllocator!GCAllocator);
    
    auto alloc = GCAllocator();
    auto i = alloc.create!int(1);
    assert(*i == 1);
    
    static struct HasCtor 
    {
        int i;
        
        this(int i) { this.i = i; }
    }
    
    static struct NoCtor
    {
        int i;
    }
    
    auto hc = alloc.create!HasCtor(42);
    assert(hc.i == 42);
    
    auto nc = alloc.create!NoCtor(42);
    assert(nc.i == 42);
}
