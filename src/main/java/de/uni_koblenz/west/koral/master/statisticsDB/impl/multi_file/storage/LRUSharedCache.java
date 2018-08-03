package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

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
		while (!sharedSpaceManager.isAvailable(entrySize) && !isEmpty()) {
			removeEldest();
		}
		// At this point, either we made enough space by removing the eldest or this cache is empty and the manager has
		// to free up space now.
		sharedSpaceManager.request(sharedSpaceConsumer, entrySize);
		super.put(key, value);
	}

	@Override
	public void remove(K key) {
		super.remove(key);
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
