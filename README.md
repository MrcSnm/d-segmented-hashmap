# d-segmented-hashmap
A segmented hash map implementation for D which provides must faster table creation

### Implementatio: Almost implements every feature from D AA:
- Stable location so, you can hold up any key/value elocation

### Improvements over D AA
- (Better for fire and forget) -> No rehash happens 
- Might use less memory, specially for strings as keys since the state is encoded on it
- `put` is a much faster operation, almost tripling the speed of D AA
- You can pre-initialize its capacity (reserve memory)

### Cons:
- A `get` operation with best case of `O(1)` and worst case of `O(log n)`. Though it will never be `O(n)` since no rehash happens
- After removing elements, other segmentations of the map will become unused. - This maybe can be improved




## Usage

```d
import hip.util.shashmap;
HashMap!(string, string) map;
//map.setCapacity(50_000); //You may use that for reserving the size
foreach(i; 0..50000)
{
    map[identifiers[i]] = values[i];
}

foreach(string value; map)
{
    target = value;
}

string* myValue = "someKey" in map;
map.remove("someKey");

```

