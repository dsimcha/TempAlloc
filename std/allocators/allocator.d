/**
This module contains the definition of the D allocator interface.  An
allocator is a struct or clas that encapsulates allocating and freeing of 
blocks of memory.  The allocator interface is defined structurally by the 
$(D isAllocator) template.  Additionally, a dynamic (runtime) interface
that automatically wraps the low-level, non-templated allocator functionality
is provided by the $(D DynamicAllocator) interface.

Author:  David Simcha
License: $(WEB boost.org/LICENSE_1_0.txt, Boost License 1.0).
*/
module std.allocators.allocator;

import std.conv;
import std.range;
import std.traits;
import std.typetuple;

/**
Tests whether A conforms to the structural interface of an allocator.  An
allocator must be a class or struct with member functions/templates and
properties with the following signatures and semantics:

---
// Allocate a raw block of memory of size nBytes.
void* allocate(size_t nBytes);  

 // Free a block of memory pointed to by ptr.
void* free(void* ptr);            

// Attempt to resize a block of memory pointed to by ptr to size nBytes
// in place.  Return true if successful, false if not.
bool resize(void* ptr, size_t nBytes);    

// Whether pointers passed to free() are checked for validity.  Must be
// computable at compile time.
bool freeIsChecked;

// The number of bytes to which an allocation of nBytes is guaranteed to be
// aligned.  Must be static and computable at compile time.
size_t alignBytes(size_t nBytes);

// The amount of space allocated to satisfy a request of size nBytes.
// This may be larger than nBytes due to alignment, etc.  Must be
// computable at compile time.
size_t allocSize(size_t nBytes);

// True if the memory allocated by this allocator is reclaimed automatically
// via garbage collection.  If this is true, free() may be a no-op.
bool isAutomatic;

// True if memory allocated by this allocator is freed when the allocator
// goes out of scope.
bool isScoped;

// Create a new instance of a class on the allocator, passing args to the
// constructor.
T create(T, Args)(auto ref Args args);

// Create a new instance of a struct or primitive on the allocator, passing
// args to the constructor.
T create(T, Args)(auto ref Args args);

// Create a new array of type T with dimensions sizes.  See 
// TypedAllocatorMixin for an example.
T newArray(T, I...)(I sizes);

// Same as newArray but do not initialize elements.
T uninitializedArray(T, I...)(I sizes);

// Copy a generic range to an array.
ElementType!(R)[] array(R)(R range);

// Obtain an instance of a DynamicAllocator object that uses this allocator.
// This must be obtained from the allocator instance so that the
// allocator has control over how the DynamicAllocator object is allocated.
@property DynamicAllocator dynamicAllocator();
---
*/
template isAllocator(A) 
{
    enum bool isAllocator = is(typeof({
        A a;
        void* ptr = a.allocate(42);
        a.free(ptr);
        bool res = a.resize(ptr, 50);
        bool fic = a.freeIsChecked;
        size_t ab = a.alignBytes(42);
        size_t as = a.allocSize(42);
        bool ia = a.isAutomatic;
        bool isc = a.isScoped;
        
        static class C {}
        C c = a.create!C();
        
        static struct S {}
        S* s = a.create!S();
        
        int[][] ii = a.newArray!(int[][])(3, 2);
        ii = a.uninitializedArray!(int[][])(3, 2);
        ii = a.array(ii);
        
        DynamicAllocator dyn = a.dynamicAllocator;
    }));
}
        
/**
This interface provides runtime polymorphism for the non-templated portions
of the structural interface of allocator.  The class 
$(D DynamicAllocatorTemplate) automatically wraps any object conforming to
the structural allocator interface in an object inheriting from this
runtime polymorphic interface.  

Example:
---
import std.allocators.gcallocator;

auto gcAlloc = GCAllocator();
DynamicAllocator dynamic = gcAlloc.dynamicAllocator;
assert(dynamic.allocate(42));
---
*/
interface DynamicAllocator 
{
    /**
    Allocator interface as described in isAllocator, excluding templated 
    convenience functions and compile-time computability.
    */
    void* allocate(size_t);
    
    /// Ditto
    void free(void*);
    
    /// Ditto
    bool resize(void*, size_t);
    
    /// Ditto
    @property bool freeIsChecked();
    
    /// Ditto
    size_t alignBytes(size_t);

    /// Ditto
    size_t allocSize(size_t);

    /// Ditto
    @property bool isAutomatic();
        
    /// Ditto
    @property bool isScoped();
}

/**
This class wraps any object conforming to the structural allocator interface
in a runtime polymorphic object conforming to the $(D DynamicAllocator)
runtime interface.  It should only be used when creating a new allocator
type.

Examples:
---
struct SomeAllocator 
{
    @property DynamicAllocator dynamicAllocator()
    {
        return new DynamicAllocatorTemplate!(SomeAllocator)(this);
    }
}
---
*/
class DynamicAllocatorTemplate(A) : DynamicAllocator 
{
    private A baseAlloc;
    
    /// Construct a DynamicAllocatorTemplate with alloc as its base allocator.
    this(A baseAlloc) {
        this.baseAlloc = baseAlloc;
    }
    
    override void* allocate(size_t nBytes) 
    { 
        return baseAlloc.allocate(nBytes); 
    }    
    
    override void free(void* ptr) 
    { 
        return baseAlloc.free(ptr); 
    }
    
    override bool resize(void* ptr, size_t nBytes) 
    { 
        return baseAlloc.resize(ptr, nBytes); 
    }

    override @property bool freeIsChecked()
    {
        return baseAlloc.freeIsChecked;
    }
    
    override size_t alignBytes(size_t nBytes)
    {
        return baseAlloc.alignBytes(nBytes);
    }

    override size_t allocSize(size_t nBytes)
    {
        return baseAlloc.allocSize(nBytes);
    }

    override @property bool isAutomatic() 
    {
        return baseAlloc.isAutomatic;
    }
        
    override @property bool isScoped()
    {
        return baseAlloc.isScoped;
    }
}

/**
This mixin provides default implementations of the high-level templated
allocator functions in terms of the low-level non-templated functions.
An allocator may use these or may include its own implementations of this
functionality.
*/
mixin template TypedAllocatorMixin()
{
    /**
    Create a new instance of a class on the current allocator,
    passing $(D args) to its constructor.
    
    Examples:
    ---
    import std.allocators.region;
    
    class C {}
    
    void main() 
    {
        auto alloc = newRegionAllocator();
        auto c = alloc.create!C();
    }
    ---
    */
    C create(C, Args...)(auto ref Args args) if(is(C == class))
    {
        enum size = __traits(classInstanceSize, C);
        auto chunk = allocate(size)[0..size];
        return emplace!C(chunk, args);
    }       
    
    /**
    Create a new instance of a non-class type on the current allocator,
    passing $(D args) to the constructor in the case of a struct or
    initializing the value to $(D args[0]) if $(D args.length == 1) and
    $(D T) is a primitive.
    */
    T* create(T, Args...)(auto ref Args args) if(!is(T == class))
    {
        auto chunk = cast(T*) allocate(T.sizeof);
        return emplace!T(chunk, args);
    }
    
    /**
    Allocates an array of type $(D T).  $(D T) may be a multidimensional 
    array.  In this case sizes may be specified for any number of dimensions 
    from 1 to the number in $(D T).

    Examples:
    ---
    import std.allocators.region;
    
    void main()
    {    
        auto alloc = newRegionAllocator();
        double[] arr = alloc.newArray!(double[])(100);
        assert(arr.length == 100);

        double[][] matrix = alloc.newArray!(double[][])(42, 31);
        assert(matrix.length == 42);
        assert(matrix[0].length == 31);
    }
    ---
    */
    auto newArray(T, I...)(I sizes)
    if(allSatisfy!(isIntegral, I)) 
    {

        static void initialize(R)(R toInitialize) {
            static if(isArray!(ElementType!R)) {
                foreach(elem; toInitialize) {
                    initialize(elem);
                }
            } else {
                toInitialize[] = ElementType!(R).init;
            }
        }

        auto ret = uninitializedArray!(T, I)(sizes);
        initialize(ret);
        return ret;
    }
    
    /**
    Same as $(D newArray), except skips initialization of elements for
    occasions when greater performance is required.
    */
    auto uninitializedArray(T, I...)(I sizes)
    if(allSatisfy!(isIntegral, I)) {
        static assert(sizes.length >= 1,
            "Cannot allocate an array without the size of at least the first " ~
            " dimension.");
        static assert(sizes.length <= nDimensions!T,
            to!string(sizes.length) ~ " dimensions specified for a " ~
            to!string(nDimensions!T) ~ " dimensional array.");

        alias typeof(T.init[0]) E;

        auto ptr = cast(E*) allocate(sizes[0] * E.sizeof);
        auto ret = ptr[0..sizes[0]];

        static if(sizes.length > 1) {
            foreach(ref elem; ret) {
                elem = uninitializedArray!(E)(sizes[1..$]);
            }
        }

        return ret;
    }
    
    /**
    Copies an input range to an array allocated on the current allocator.
    */
    ElementType!(R)[] array(R)(R range) if(isInputRange!R && !isInfinite!R)
    {
        static if(hasLength!R)
        {
            import std.algorithm;
            auto ret = uninitializedArray!(typeof(return))(range.length);
            copy(range, ret);
            return ret;
        }
        else
        {
            // Use simple geometric growth scheme where resizing doesn't
            // work, initial size of 8 elements.
            auto ret = uninitializedArray!(typeof(return))(8);
            alias ElementType!R E;
            
            size_t i = 0;
            foreach(elem; range) 
            {
                scope(exit) i++;
                if(ret.length > i) 
                {
                    emplace(ret.ptr + i, elem);
                }
                else if(resize(cast(void*) ret.ptr, (ret.length + 8) * E.sizeof))
                {
                    // Try to resize by at least 8 elements.  This is 
                    // somewhat arbitrary but prevents the possibly slow
                    // resize() from being called constantly.
                    ret = ret.ptr[0..i + 1];
                    emplace(ret.ptr + i, elem);
                }
                else
                {
                    // Attempte to double the size of ret, free the old 
                    // version, etc.  This will fail with some allocators,
                    // for example RegionAllocator, but they should have 
                    // their own scheme for dealing with this.
                    auto ret2 = uninitializedArray!(typeof(return))
                        (ret.length * 2);
                    memcpy(cast(void*) ret2.ptr, ret.ptr, ret.length * E.sizeof);
                    free(cast(void*) ret.ptr);
                    ret = ret2;
                    emplace(ret.ptr + i, elem);
                }
            }
            
            return ret[0..i];
        }
    }
}

// Create a really stupid allocator for testing all TypedAllocatorMixin,
// DynamicAllocator, isAllocator.
version(unittest)
{   
    struct StupidAllocator
    {
        void* allocate(size_t nBytes) 
        {
            return (new byte[nBytes]).ptr;
        }
        
        void free(void* ptr) {}  // Let the GC handle it.
        
        static bool resize(void* ptr, size_t newSize) 
        {
            import core.memory;
            immutable bytesNeeded = newSize - GC.sizeOf(ptr);
            immutable result = GC.extend(ptr, bytesNeeded, bytesNeeded);
            return result >= newSize;
        }
             
        enum freeIsChecked = false;
        
        enum isAutomatic = true;
    
        enum isScoped = false;
        
        size_t allocSize(size_t nBytes) { return 42; }
        size_t alignBytes(size_t nBytes) { return 42; }
        
        DynamicAllocator dynamicAllocator() @property
        {
            return new DynamicAllocatorTemplate!StupidAllocator(this);
        }
        
        mixin TypedAllocatorMixin;
    }
}
    
unittest
{
    static assert(isAllocator!StupidAllocator);
    import std.stdio;
    StupidAllocator alloc;
    auto arr = [8, 6, 7, 5, 3, 0, 9];
    auto arr2 = alloc.array(arr);
    assert(arr2 == arr);
    
    import std.algorithm, std.range;
    auto filtered = filter!"a % 7 == 0"(iota(100));
    auto arr3 = alloc.array(filtered);
    assert(arr3 == 
        [0, 7, 14, 21, 28, 35, 42, 49, 56, 63, 70, 77, 84, 91, 98]
    );
    
    static struct S {}
    auto s = alloc.create!S();
    
    static class C {}
    auto c = alloc.create!C();
    
    auto d = alloc.dynamicAllocator;
    d.allocate(42);
}    
