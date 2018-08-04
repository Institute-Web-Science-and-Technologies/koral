package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.FileManager;

public class SharedSpaceManager {

	private final long maxSize;

	private long used;

	private final Map<SharedSpaceConsumer, Long> consumers;

	private final FileManager fileManager;

	public SharedSpaceManager(FileManager fileManager, long maxSize) {
		this.fileManager = fileManager;
		this.maxSize = maxSize;

		consumers = new HashMap<>();
	}

	public boolean isAvailable(long amount) {
		return amount < (maxSize - used);
	}

	public boolean request(SharedSpaceConsumer consumer, long amount) {
		long available = maxSize - used;
		if (available < 0) {
			throw new IllegalStateException("Too many resources in use");
		}
		if (available < amount) {
			Iterator<Entry<Long, ExtraRowStorage>> extraFiles = fileManager.getLRUExtraFiles();
			fileLoop: while (extraFiles.hasNext()) {
				ExtraRowStorage extraFile = extraFiles.next().getValue();
				while (extraFile.makeRoom()) {
					// Free up space until either enough is available or is nothing left to free up
					if ((maxSize - used) >= amount) {
						break fileLoop;
					}
				}
			}
		}
		if ((maxSize - used) < amount) {
			throw new OutOfMemoryError("Could not free up enough space. In use: " + used + ", requested: " + amount);
		}
		used += amount;

		Long consumerUsed = consumers.get(consumer);
		if (consumerUsed == null) {
			consumerUsed = 0L;
		}
		consumerUsed += amount;
		consumers.put(consumer, consumerUsed);

		return true;
	}

	public void release(SharedSpaceConsumer consumer, long amount) {
		used -= amount;
		if (used < 0) {
			throw new IllegalStateException("Too many resources released");
		}

		Long consumerUsed = consumers.get(consumer);
		if (consumerUsed == null) {
			throw new IllegalArgumentException("Consumer " + consumer + " has no allocated space to release");
		}
		consumerUsed -= amount;
		consumers.put(consumer, consumerUsed);
	}

	public void releaseAll(SharedSpaceConsumer consumer) {
		Long consumerUsed = consumers.remove(consumer);
		if (consumerUsed != null) {
			used -= consumerUsed;
		}
		System.out.println("Used after releaseAll: " + used);
	}

}
