module hip.util.shashmap;

enum DefaultInitSize = 8;
struct HashMap(K, V)
{
	private enum SeparateSlotState = !is(K == string);
    struct KV
    {
        static if(!SeparateSlotState)
            SString key;
        else
            K key;
        V value;
    }
	private KV* keyValues;
    private HashMap!(K, V)* maps;
	private uint capacity;
	uint length;
	private ubyte actualMapsCount, mapsCount;
	private bool uniqueOnly;
	private uint currentCapacity;
	private float resizeFactor;


	static if(SeparateSlotState)
	{
		private SlotState* states;
		SlotState getState(const(SlotState)* stateArr, size_t index) const
		{
			auto arrayIndex = index >> 2;
			SlotState s = stateArr[arrayIndex];
			ubyte bitIndex = index & 0b11;
			return cast(SlotState)((s >> bitIndex*2) & 0b11);
		}

		pragma(inline, true) SlotState getState(size_t index) const
		{
			return getState(states, index);
		}

		pragma(inline, true) bool isDeadOrEmpty(size_t index) const
		{
			return getState(states, index) != SlotState.alive;
		}
		void setState(size_t index, SlotState state)
		{
			auto arrayIndex = index >> 2;
			ubyte bitIndex = (index & 0b11) * 2; 
			states[arrayIndex] &= ~(0b11 << bitIndex);  // Clear 2 bits
			states[arrayIndex] |= (state & 0b11) << bitIndex;  // Set 2 bits

		}
		private pragma(inline) size_t getRequiredStateCount() const
		{
			return (capacity + 3) >> 2;
		}
	}
	else
	{
		pragma(inline, true)
		{
			void setState(size_t index, SlotState state)
			{
				keyValues[index].key.setExtra(state);

				assert(getState(index) == state, " Set state false ");
			}
			SlotState getState(const(KV)* keysArr, size_t index) const
			{
				return cast(SlotState)(keysArr[index].key.extra);
			}

			bool isDeadOrEmpty(const(KV)* keysArr, size_t index) const
			{
				return (keysArr[index].key.isDeadOrEmpty);
			}

			SlotState getState(size_t index) const
			{
				return getState(keyValues, index);
			}
		}
	}

	void setCapacity(size_t capacity = DefaultInitSize)
	{
		import core.memory;
        // assert(this.capacity == 0, "Can only set a map capacity once.");
		this.capacity = cast(uint)capacity;
		keyValues = cast(KV*)GC.malloc(KV.sizeof*capacity);
		

		static if(SeparateSlotState)
		{
			states = cast(SlotState*)GC.malloc(getRequiredStateCount, GC.BlkAttr.NO_SCAN);
			states[0..getRequiredStateCount] = SlotState.empty;
		}
		else
		{
			keyValues[0..capacity] = KV.init;
		}
		currentCapacity = cast(uint)getStructureCapacity(capacity, mapsCount);
		resizeFactor = getResizeFactor(cast(uint)capacity);

	}
    static size_t getStructureCapacity(size_t capacity, size_t maps)
    {
        return capacity * ((GrowthFactor ^^ (maps + 1)) - 1);
    }

    private void branch()
    {
        import core.memory;
        mapsCount++;
        if(maps == null)
		{
			actualMapsCount++;
            maps = cast(HashMap!(K, V)*)GC.malloc(mapsCount* HashMap!(K, V).sizeof);
		}
        else if(mapsCount > actualMapsCount)
		{
			actualMapsCount = mapsCount;
            maps = cast(HashMap!(K, V)*)GC.realloc(maps, actualMapsCount* HashMap!(K, V).sizeof);
		}
		else
			return;
        HashMap!(K, V)* lastMap = mapsCount == 1 ? &this : &maps[mapsCount-2];
        maps[mapsCount-1].setCapacity(lastMap.capacity * GrowthFactor);
		currentCapacity = cast(uint)getStructureCapacity(capacity, mapsCount);
		resizeFactor = getResizeFactor(capacity);
    }

	static pragma(inline, true) size_t getHash(K key)
	{
		static if(is(K == string))
			// return xxhash(cast(ubyte*)key.ptr, key.length);
			return hash_64_fnv1a(key.ptr, cast(ulong)key.length);
		else 
			// return xxhash(cast(ubyte*)&key, key.sizeof);
			return hash_64_fnv1a(&key, cast(ulong)key.sizeof);
	}

	ref inout(V) opIndex(K key) inout
	{
		return *get(key);
	}
	inout(V)* opBinary(string op)(const K key) inout if(op == "in")
	{
		return get(key);
	}
	alias opBinaryRight = opBinary;
	auto opIndexAssign(V value, K key)
	{
		put(key, value);
		return value;
	}

	static pragma(inline, true) bool isDeadOrEmpty(HashMap!(K, V)* current, size_t hash, KV* kv)
	{
		static if(SeparateSlotState)
			return current.isDeadOrEmpty(hash);
		else
			return kv.key.isDeadOrEmpty();
	}

	void assumeUniqueKeys(bool assume) { uniqueOnly = assume; }
	bool isAssumingUniqueKeys() const { return uniqueOnly; }

	/**
	 * Params:
	 *   key = The key to set
	 *   value = The value to set
	 * Returns: Whether the length has increased or not
	 */
	bool uncheckedPut(K key, V value)
	{
		size_t hash = getHash(key);
        HashMap!(K, V)* current = mapsCount == 0 ? &this : &maps[mapsCount-1];
		KV* currentKv = current.keyValues;
        size_t currCapacity = current.capacity;
		size_t currHash = hash % currCapacity;
        int probeCount = 0;

		size_t maxProbes = getMaxProbes(currCapacity);
		while(true)
		{
			KV* kv = &currentKv[currHash];
			if(isDeadOrEmpty(current, currHash, kv))
			{
				static if(SeparateSlotState)
				{
					*kv = KV(key, value);
					current.setState(currHash, SlotState.alive);
				}
				else
					*kv = KV(SString.create(key.length, key.ptr, SlotState.alive), value);
				return true;
			}
			else if(!uniqueOnly)
			{
				if(kv.key == key)
				{
					kv.value = value;
					return false;
				}
			}
            if(probeCount++ == maxProbes)
            {
                probeCount = 0;
                branch();
                current = &maps[mapsCount - 1];
                currentKv = current.keyValues;
                currCapacity = current.capacity;
				maxProbes = getMaxProbes(currCapacity);
            }
			currHash = (hash + probeCount) % currCapacity;
		}
	}
	void put(K key, V value)
	{
		if(capacity == 0)
			setCapacity(DefaultInitSize);

		// if((length > UseCollisionRateThreshold && cast(float)collisionsInLength / length > CollisionFactor) ||
        if(cast(float)(length + 1) / currentCapacity > resizeFactor)
        {
			branch();
        }
		if(uncheckedPut(key, value))
			length++;
	}

	inout(V)* get(K key) inout
	{
		if(capacity == 0)
			return null;
		size_t hash = getHash(key);
        int currentMap = mapsCount - 1;
        HashMap!(K, V)* current = currentMap == -1 ? cast(HashMap!(K, V)*)&this : cast(HashMap!(K, V)*)&maps[currentMap];
		while(true)
		{
            size_t currCapacity = current.capacity;
            size_t maxProbes = getMaxProbes(currCapacity);

            for (size_t probeCount = 0; probeCount <= maxProbes; probeCount++)
            {
                size_t cHash = (hash + probeCount) % currCapacity;
                if (current.getState(cHash) != SlotState.alive)
                    break;
                if(current.keyValues[cHash].key == key)
                    return cast(inout)&(current).keyValues[cHash].value;
            }
            if(currentMap == -1)
                return null;
            currentMap--;
            current = currentMap == -1 ? cast(HashMap!(K, V)*)&this : cast(HashMap!(K, V)*)&maps[currentMap];
		}
		return null;
	}


	K[] keys()
	{
		import core.memory;
		auto ret = cast(K*)GC.malloc(K.sizeof*length);
		size_t index = 0;
		HashMap!(K, V)* current = &this;
		for(size_t currentMap = 0; currentMap < mapsCount; currentMap++)
		{
			foreach(i; 0..current.capacity)
			{
				if(current.getState(i) == SlotState.alive)
					ret[index++] = current.keyValues[i].key;
			}
			current = &maps[currentMap];
		}
		return ret[0..length];
	}
	V[] values()
	{
		import core.memory;
		auto ret = cast(V*)GC.malloc(V.sizeof*length);
		size_t index = 0;
		HashMap!(K, V)* current = &this;
		for(size_t currentMap = 0; currentMap < mapsCount; currentMap++)
		{
			foreach(i; 0..current.capacity)
			{
				if(current.getState(i) == SlotState.alive)
					ret[index++] = current.keyValues[i].value;
			}
			current = &maps[currentMap];
		}
		return ret[0..length];
	}

	void clear()
	{
		HashMap!(K, V)* current = &this;
		for(size_t currentMap = 0; currentMap < mapsCount; currentMap++)
		{
			foreach(i; 0..current.capacity)
				current.setState(i, SlotState.empty);
			current = &maps[currentMap];
		}
		mapsCount = 0;
		length = 0;
	}

	void remove(K key)
	{
		const size_t precalcHash = getHash(key);
		int currentMap = mapsCount - 1;
        HashMap!(K, V)* current = currentMap == -1 ? cast(HashMap!(K, V)*)&this : cast(HashMap!(K, V)*)&maps[currentMap];
		while(true)
		{
			import std.stdio;
			size_t currCapacity = current.capacity;
			size_t maxProbes = getMaxProbes(currCapacity);

			int probeIndex = -1;
			for (int probeCount = 0; probeCount <= maxProbes; probeCount++)
			{
				size_t cHash = (precalcHash + probeCount) % currCapacity;
				if (current.getState(cHash) == SlotState.empty)
					break;
				else if(current.keyValues[cHash].key == key)
				{
					probeIndex = probeCount;
					break;
				}
			}
			if(probeIndex != -1)
			{
				current.setState((precalcHash + probeIndex) % currCapacity, SlotState.dead);
				break;
			}
			if(currentMap == -1)
			{
				assert(false, "Member not found.");
				return;
			}
			currentMap--;
			current = currentMap == -1 ? cast(HashMap!(K, V)*)&this : cast(HashMap!(K, V)*)&maps[currentMap];
		}
		length--;
	}

	int opApply(scope int delegate(K key, ref V value) dg) const
	{
		int result = 0;
		int index = 0;
		int count = 0;
		int mapIndex = 0;
		const(HashMap!(K, V))* currMap = &this;

		while(count < length)
		{
			if(index == currMap.capacity)
			{
				assert(maps != null, "Null maps? Did something happen?");
				currMap = &maps[mapIndex++];
				index = 0;
			}

			if(currMap.getState(index) == SlotState.alive)
			{
				count++;
				result = dg(cast()currMap.keyValues[index].key, cast()currMap.keyValues[index].value);
				if (result)
					break;
			}
			index++;
		}
		return result;
	}
	int opApply(scope int delegate(ref V value) dg) const
	{
		int result = 0;
		int index = 0;
		int count = 0;
		int mapIndex = 0;
		const(HashMap!(K, V))* currMap = &this;

		while(count < length)
		{
			if(index == currMap.capacity)
			{
				currMap = &maps[mapIndex++];
				index = 0;
			}
			if(currMap.getState(index) == SlotState.alive)
			{
				count++;
				result = dg(cast()currMap.keyValues[index].value);
				if (result)
					break;
			}
			index++;
		}
		return result;
	}

	private auto entryRange(alias entryOp)() inout
	{
		static struct EntryRange
		{
			HashMap!(K, V)* map;
			HashMap!(K, V)* currMap;
			uint length, index, count;
			uint mapIndex;

			void popFront()
			{
				if(index + 1 >= currMap.capacity && mapIndex != map.mapsCount)
				{
					currMap = &map.maps[mapIndex];
					mapIndex++;
					index = 0;
				}
				else
					index++;
			}

			auto front()
			{
				count++;
				while(currMap.getState(index) != SlotState.alive)
				{
					if(index + 1 >= currMap.capacity && mapIndex != map.mapsCount)
					{
						currMap = &map.maps[mapIndex];
						index = 0;
						mapIndex++;
						break;
					}
					index++;
				}
				return entryOp(currMap.keyValues[index]);
			}
			bool empty() => count == length;
		}
		return EntryRange(cast(HashMap!(K, V)*)&this, cast(HashMap!(K, V)*)&this, length, 0, 0);
	}
	auto byKey() inout
	{
		return entryRange!((ref KV kv) => kv.key);
	}
	auto byValue() inout
	{
		return entryRange!((ref KV kv) => kv.value);
	}

	auto byKeyValue() inout
	{
		return entryRange!((ref KV kv) => kv);
	}
}

private:

uint hash_32_fnv1a(const void* key, const uint len) {

    const(char)* data = cast(char*)key;
    uint hash = 0x811c9dc5;
    uint prime = 0x1000193;

    for(int i = 0; i < len; ++i) {
        hash = (hash ^ data[i]) * prime;
    }

    return hash;

} //hash_32_fnv1a

ulong hash_64_fnv1a(const void* key, const ulong len) {
    
    enum ulong prime = 0x100000001b3;
    ulong hash = 0xcbf29ce484222325;
    const(char)* data = cast(char*)key;
    
    for(int i = 0; i < len; ++i) {
        hash = (hash ^ data[i]) * prime;
    }
    
    return hash;
} 

enum SlotState : ubyte
{
	empty = 0,
	alive = 1,
	dead = 0b10
}

struct SString
{
	size_t length;
	private immutable(char)* ptr;
	enum size_t mask = 0b11UL << 62;
	enum size_t isAliveBit = cast(size_t)SlotState.alive << 62;

	pragma(inline, true)
	string toString() const
	{
		string ret;
		(cast(size_t*)&ret)[0] = length & ~mask;
		(cast(size_t*)&ret)[1] = cast(size_t)ptr;
		
		return ret;
	}

	pragma(inline, true) static SString create(size_t length, immutable(char)* ptr, ubyte ex)
	{
		assert(ex <= 0b11, "Extra too big.");
		return SString(length | (cast(size_t)ex << 62), ptr);
	}

	pragma(inline, true)
	SString opAssign(string other)
	{
		length = other.length | extra;
		ptr = cast(immutable(char)*)other.ptr;
		return this;
	}

	pragma(inline, true) bool isDeadOrEmpty() const
	{
		return !(length & isAliveBit);
	}

	pragma(inline, true)  bool opEquals(string other) const { return other == toString; }
	pragma(inline, true) ubyte extra() const { return cast(ubyte)((length & mask) >> 62);}

	pragma(inline, true) void setExtra(ubyte ex)
	{
		assert(ex <= 0b11, "Extra too big.");
		length = (length & ~(0b11UL << 62)) | (cast(size_t)ex) << 62;
	}
	alias toString this;
}

private	enum GrowthFactor = 2;

float getResizeFactor(uint capacity)
{
    if(capacity <= 16)
        return 1.0;
    else if(capacity <= 32)
        return 0.85;
    else if(capacity <= 1024)
        return 0.75;
    return 0.66;
}


size_t getMaxProbes(size_t capacity)
{
    if(capacity <= 1024)
        return 4;
    if(capacity <= 4096)
        return 8;
    if(capacity <= 16384)
        return 16;
    return 32;
}


unittest
{
	HashMap!(string, string) test;
	test["hello"] = "world";
	test["hello"] = "brother";

	foreach(k, v; test)
	{
		assert(v == "brother");
	}

	assert(test.length == 1, "Failed at length test");
}