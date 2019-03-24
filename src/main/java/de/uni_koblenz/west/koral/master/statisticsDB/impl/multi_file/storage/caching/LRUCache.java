package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching;

/**
 * A generic LRU cache with O(1) operations. Uses a doubly-linked-list for access order plus an index (Map) for O(1)
 * access. Differs from LRUList by enforcing a given capacity limit.
 *
 * @author philipp
 *
 * @param <K>
 * @param <V>
 */
public class LRUCache<K, V> extends LRUList<K, V> {

	private final long capacity;

	public LRUCache(long capacity) {
		super();
		this.capacity = capacity;
	}

	@Override
	public void put(K key, V value) {
		if (list.size() == capacity) {
			evict();
		}
		assert list.size() < capacity : "List: " + list.size() + ", capacity: " + capacity;
		super.put(key, value);
	}

	/**
	 * @return The amount of elements in the cache, that is the doubly linked list. There may be more in the index map,
	 *         depending on sub-implementations of {@link #removeEldest(DoublyLinkedNode)}.
	 */
	@Override
	public long size() {
		return list.size();
	}

	@Override
	public void clear() {
		super.clear();
	}

	@Override
	public String toString() {
		return super.toString() + "\nSize: " + list.size();
	}

}
