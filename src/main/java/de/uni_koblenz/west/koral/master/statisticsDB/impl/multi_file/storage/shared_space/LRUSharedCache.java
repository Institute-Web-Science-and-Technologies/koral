package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.shared_space;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching.LRUList;

/**
 * An LRU Cache that uses a {@link SharedSpaceManager} to manage how much space each consumer can have.
 *
 * @author Philipp Töws
 *
 * @param <K>
 *            Type of the key of the cache elements.
 * @param <V>
 *            Type of the value of the cache elements.
 */
public class LRUSharedCache<K, V> extends LRUList<K, V> implements AutoCloseable {

	private final SharedSpaceManager sharedSpaceManager;
	private final int entrySize;
	private final SharedSpaceConsumer sharedSpaceConsumer;

	public LRUSharedCache(SharedSpaceManager sharedSpaceManager, SharedSpaceConsumer sharedSpaceConsumer,
			int entrySize) {
		this.sharedSpaceManager = sharedSpaceManager;
		this.sharedSpaceConsumer = sharedSpaceConsumer;
		this.entrySize = entrySize;
	}

	@Override
	public void put(K key, V value) {
		if (!sharedSpaceManager.request(sharedSpaceConsumer, entrySize)) {
			// Since this is used by RARF that can make room for own requests, which should cause makeRoom() to never
			// return false, a failure means something serious
			throw new RuntimeException("Space request failed");
		}
		super.put(key, value);
	}

	/**
	 * The space used in bytes needed for storing all entries.
	 */
	public long spaceUsed() {
		return sharedSpaceManager.getSpaceUsed(sharedSpaceConsumer);
	}

	@Override
	public void remove(K key) {
		super.remove(key);
		sharedSpaceManager.release(sharedSpaceConsumer, entrySize);
	}

	@Override
	public void evict() {
		super.evict();
		sharedSpaceManager.release(sharedSpaceConsumer, entrySize);
	}

	@Override
	public void clear() {
		super.clear();
		sharedSpaceManager.releaseAll(sharedSpaceConsumer);
	}

	@Override
	public void close() {
		sharedSpaceManager.releaseAll(sharedSpaceConsumer);
	}
}
