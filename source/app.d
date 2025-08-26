import hip.util.shashmap;
import std.stdio;
import std.datetime.stopwatch;

/** 
 * Forward find
 [4 secs, 482 ms, 246 μs, and 7 hnsecs]
world49999
Paused Time: 59 ms, 925 μs, and 3 hnsecs
Collection Time: 60 ms and 185 μs
Allocated: 432 MB
Free: 28 MB
Used: 8 MB
 */


/** 
 * Backward find
[1 sec, 155 ms, 149 μs, and 3 hnsecs]
world49999
Paused Time: 135 ms, 140 μs, and 5 hnsecs
Collection Time: 137 ms, 621 μs, and 8 hnsecs
Allocated: 432 MB
Free: 14 MB
Used: 25 MB
 */
void main()
{
	import std.conv:to;
	enum tests = 100;
	string[] identifiers;
	string[] values;
	foreach(i; 0..50000)
	{
		// identifiers ~= "hello/Users/Hipreme/Documents/test/shashtest/source/shashmap.d"~i.to!string;
		identifiers ~= "hello"~i.to!string;
		values ~= "world"~i.to!string;
	}

	string target;

		// string[string] map;
		HashMap!(string, string) map;
	writeln = benchmark!(() {
		// HashMap!(string, string) map;
		// map.setCapacity(50_000);
		foreach(i; 0..50000)
		{
			map[identifiers[i]] = values[i];
		}
		// foreach(i; 0..50000)
		// {
		// 	target = map[identifiers[i]];
		// 	// writeln = target;
		// }
	})(1);

	int count = 0;
	writeln = benchmark!(()
	{
		foreach(value; map)
		{
			target = value;
			// writeln = key;
			// count++;
		}
		// foreach(key; map.byKey)
		// {
		// 	target = key;
		// 	// writeln = key;
		// 	// count++;
		// }
	})(tests);

	writeln = benchmark!(()
	{
		foreach(i; 0..50_000)
		{
			// writeln = "Removing "~identifiers[i];
			map.remove(identifiers[i]);
		}
	})(1);

	// writeln = benchmark!(()
	// {
	// 	foreach(i; 0..50_000)
	// 	{
	// 		// writeln = "Removing "~identifiers[i];
	// 		map[values[i]] = identifiers[i];
	// 	}
	// })(1);

	writeln = map.length;

	import core.memory;

	auto prof = GC.profileStats;
	auto stats = GC.stats;;
	writeln("Paused Time: ", prof.totalPauseTime);
	writeln("Collection Time: ", prof.totalCollectionTime);
	writeln("Allocated: ", stats.allocatedInCurrentThread / 1_000_000, " MB");
	writeln("Free: ", stats.freeSize / 1_000_000, " MB");
	writeln("Used: ", stats.usedSize / 1_000_000, " MB");

}
