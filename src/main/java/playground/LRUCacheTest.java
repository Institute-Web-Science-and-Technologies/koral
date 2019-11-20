package playground;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching.LRUCache;

public class LRUCacheTest {

	public static void main(String[] args) {
		LRUCache<Integer, String> cache = new LRUCache<>(10);
		for (int i = 0; i < 9; i++) {
			cache.put(i, "'" + i + "'");
		}
		System.out.println(cache);
		System.out.println("Add 10='10'");
		cache.put(10, "'10'");
		System.out.println(cache);
		System.out.println("Retrieve 1");
		cache.get(1);
		System.out.println(cache);
		System.out.println("Add 11='11'");
		cache.put(11, "'11'");
		System.out.println(cache);
		System.out.println("Retrieve 3");
		cache.get(3);
		System.out.println("Retrieve 4");
		cache.get(4);
		System.out.println(cache);
		System.out.println("Add 12='12'");
		cache.put(12, "'12'");
		System.out.println(cache);

	}

}
