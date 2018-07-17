package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

public class SharedSpaceManager {

	private final long maxSize;

	private long used;

	public SharedSpaceManager(long maxSize) {
		this.maxSize = maxSize;
	}

	public boolean request(long amount) {
		long available = maxSize - used;
		if (available < 0) {
			throw new IllegalStateException("Too many resources in use");
		}
		if (available < amount) {
			return false;
		}
		used += amount;
		return true;
	}

	public void release(long amount) {
		used -= amount;
		if (used < 0) {
			throw new IllegalStateException("Too many resources released");
		}
	}

}
