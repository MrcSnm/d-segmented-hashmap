module hip.util.shashmap;

enum DefaultInitSize = 8;
struct HashMap(K, V)
{
	private enum SeparateSlotState = !is(K == string);
	// private enum SeparateSlotState = true;

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
	private uint actualMapsCount, mapsCount;


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
		void setState(size_t index, SlotState state)
		{
			keyValues[index].key.setExtra(state);

			assert(getState(index) == state, " Set state false ");
		}
		SlotState getState(const(KV)* keysArr, size_t index) const
		{
			return cast(SlotState)(keysArr[index].key.extra);
		}
		SlotState getState(size_t index) const
		{
			return getState(keyValues, index);
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

	ref auto opIndex(K key)
	{
		return *get(key);
	}
	const ref auto opIndex(K key)
	{
		return *get(key);
	}
	auto opBinary(string op)(const K key) const if(op == "in")
	{
		return get(key);	
	}
	auto opBinary(string op)(const K key) if(op == "in")
	{
		return get(key);	
	}
	alias opBinaryRight = opBinary;

	auto opIndexAssign(V value, K key)
	{
		put(key, value);
		return value;
	}

	void uncheckedPut(K key, V value)
	{
		size_t hash = getHash(key);
        HashMap!(K, V)* current = mapsCount == 0 ? &this : &maps[mapsCount-1];
        size_t currCapacity = current.capacity;
		size_t currHash = hash % currCapacity;
        int probeCount = 0;

		while(true)
		{
			SlotState st = current.getState(currHash);
			if(st != SlotState.alive)
			{
				static if(SeparateSlotState)
					current.keyValues[currHash] = KV(key, value);
				else
					current.keyValues[currHash] = KV(SString(key.length, key.ptr), value);
				current.setState(currHash, SlotState.alive);
				return;
			}
			else
			{
				if(current.keyValues[currHash].key == key)
				{
					current.keyValues[currHash].value = value;
					return;
				}
			}
            if(probeCount++ == getMaxProbes(currCapacity))
            {
                probeCount = 0;
                branch();
                current = &maps[mapsCount - 1];
                currCapacity = current.capacity;
            }
			currHash = (hash + probeCount) % currCapacity;
		}
	}
	void put(K key, V value)
	{
		if(capacity == 0)
			setCapacity(DefaultInitSize);

		// if((length > UseCollisionRateThreshold && cast(float)collisionsInLength / length > CollisionFactor) ||
        if(
			cast(float)(length + 1) / getStructureCapacity(capacity, mapsCount) > getResizeFactor(capacity)
		)
        {
			branch();
        }
		uncheckedPut(key, value);
		length++;

	}

	inout(V)* get(K key) inout
	{
		size_t hash = getHash(key);
        int currentMap = mapsCount - 1;
        HashMap!(K, V)* current = currentMap == -1 ? cast(HashMap!(K, V)*)&this : cast(HashMap!(K, V)*)&maps[currentMap];
		while(true)
		{
            size_t currCapacity = current.capacity;
            size_t maxProbes = getMaxProbes(currCapacity);

            if(current.getState(hash % currCapacity) == SlotState.alive) 
            for (size_t probeCount = 0; probeCount <= maxProbes; probeCount++)
            {
                size_t cHash = (hash + probeCount) % currCapacity;
                if (current.getState(cHash) == SlotState.empty)
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
		size_t currentMap = 0;
		while(true)
		{
			foreach(i; 0..current.capacity)
			{
				if(current.getState(i) == SlotState.alive)
					ret[index++] = current.keyValues[i].key;
			}
			if(currentMap == mapsCount)
				break;
			current = &maps[currentMap++];
		}
		return ret[0..length];
	}
	V[] values()
	{
		import core.memory;
		auto ret = cast(V*)GC.malloc(V.sizeof*length);
		size_t index = 0;
		HashMap!(K, V)* current = &this;
		size_t currentMap = 0;
		while(true)
		{
			foreach(i; 0..current.capacity)
			{
				if(current.getState(i) == SlotState.alive)
					ret[index++] = current.keyValues[i].value;
			}
			if(currentMap == mapsCount)
				break;
			current = &maps[currentMap++];
		}
		return ret[0..length];
	}

	void clear()
	{
		HashMap!(K, V)* current = &this;
		size_t currentMap = 0;

		while(true)
		{
			foreach(i; 0..current.capacity)
				current.setState(i, SlotState.empty);
			if(currentMap == mapsCount)
				break;
			current = &maps[currentMap++];
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
			if(current.getState(precalcHash % currCapacity) != SlotState.empty) 
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
		int mapIndex = 0;
		const(HashMap!(K, V))* currMap = &this;
		foreach(i; 0..length)
		{
			if(index >= currMap.capacity)
			{
				currMap = &maps[mapIndex++];
				index = 0;
			}
			if(currMap.getState(index) == SlotState.alive)
			{
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
		int mapIndex = 0;
		const(HashMap!(K, V))* currMap = &this;
		foreach(i; 0..length)
		{
			if(index >= currMap.capacity)
			{
				currMap = &maps[mapIndex++];
				index = 0;
			}
			if(currMap.getState(index) == SlotState.alive)
			{
				result = dg(cast()currMap.keyValues[index].value);
				if (result)
					break;
			}
			index++;
		}
		return result;
	}

	private auto entryRange(alias entryOp)()
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
		return EntryRange(&this, &this, length, 0, 0);
	}
	auto byKey()
	{
		return entryRange!((ref KV kv) => kv.key);
	}
	auto byValue()
	{
		return entryRange!((ref KV kv) => kv.value);
	}

	auto byKeyValue()
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

private enum SlotState : ubyte
{
	empty = 0,
	alive = 1,
	dead = 0b10
}

private struct SString
{
	size_t length;
	private immutable(char)* ptr;
	enum size_t mask = 0b11UL << 62;

	pragma(inline, true)
	string toString() const
	{
		string ret;
		(cast(size_t*)&ret)[0] = length & ~mask;
		(cast(size_t*)&ret)[1] = cast(size_t)ptr;
		
		return ret;
	}

	pragma(inline, true)
	SString opAssign(string other)
	{
		length = other.length | extra;
		ptr = cast(immutable(char)*)other.ptr;
		return this;
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
