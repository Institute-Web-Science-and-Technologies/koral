package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

public class LRUSharedCache<K, V> extends LRUList<K, V> implements SharedSpaceUser, AutoCloseable {

	private final SharedSpaceManager sharedSpaceManager;
	private final int entrySize;

	public LRUSharedCache(SharedSpaceManager sharedSpaceManager, int entrySize) {
		this.sharedSpaceManager = sharedSpaceManager;
		this.entrySize = entrySize;
	}

	@Override
	public void put(K key, V value) {
		if (!sharedSpaceManager.isAvailable(entrySize)) {
			if (!makeRoom()) {
				// Request space from sharedSpaceManager with force
			}
		}
		sharedSpaceManager.request(this, entrySize);
		super.put(key, value);
	}

	@Override
	public void remove(K key) {
		super.remove(key);
		sharedSpaceManager.release(this, entrySize);
	}

	@Override
	public boolean makeRoom() {
		if (tail == null) {
			return false;
		}
		removeEldest();
		return true;
	}

	@Override
	public void clear() {
		super.clear();
		sharedSpaceManager.releaseAll(this);
	}

	public boolean isEmpty() {
		return tail == null;
	}

	@Override
	public void close() {
		sharedSpaceManager.releaseAll(this);
	}
}
