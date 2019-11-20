package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.shared_space;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.FileManager;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.ExtraRowStorage;

/**
 * Manages a given amount of space that is shared between different {@link SharedSpaceConsumer}, by handling their
 * requests for more space and making room by asking the consumers if it is required.
 * 
 * @author Philipp Töws
 *
 */
public class SharedSpaceManager {

	protected final long maxSize;

	protected long used;

	/**
	 * Maps from consumers to their used space.
	 */
	protected final Map<SharedSpaceConsumer, Long> consumers;

	protected final FileManager fileManager;

	public SharedSpaceManager(FileManager fileManager, long maxSize) {
		this.fileManager = fileManager;
		this.maxSize = maxSize;

		consumers = new HashMap<>();
	}

	public boolean isAvailable(long amount) {
		return amount <= (maxSize - used);
	}

	/**
	 * Request an amount of space from the manager. It will do anything it can to make space, by asking other consumers
	 * to make room until enough space is free. It might even ask the file that requested the space to make room. This
	 * might be disabled for certain subimplementations by returning false for
	 * {@code SharedSpaceConsumer.isAbleToMakeRoomForOwnRequests()}.
	 *
	 * @param consumer
	 *            The consumer object that requests this space, for book keeping.
	 * @param amount
	 *            How much space is needed
	 * @return True if space was succesfully freed. The base implementation is supposed to always returns true.
	 */
	public boolean request(SharedSpaceConsumer consumer, long amount) {
		long available = maxSize - used;
		if (available < 0) {
			throw new IllegalStateException("Max size limit is violated");
		}
		if (available < amount) {
			boolean success = makeRoom(consumer, amount);
			if (!success) {
				return false;
			}
		}
		if ((maxSize - used) < amount) {
			throw new OutOfMemoryError("Could not free up enough space. In use: " + used + ", requested: " + amount);
		}
		used += amount;

		Long consumerUsed = getSpaceUsed(consumer);
		consumerUsed += amount;
		consumers.put(consumer, consumerUsed);

		return true;
	}

	/**
	 * Attempt to make room to allow a new entry in the cache. If there is still not enough memory after this method
	 * returned, {@link #request(SharedSpaceConsumer, long)} will throw an OOM exception.
	 *
	 * Default implementation asks each extra file, starting from the least recently used, to make room until the
	 * requested amount is free.
	 *
	 * @param requester
	 *            The consumer that requests space
	 * @param amount
	 *            The requested amount of bytes
	 * @return True if the attempt was succesful and there is now at least the given {@code amount} of space free.
	 */
	protected boolean makeRoom(SharedSpaceConsumer requester, long amount) {
		Iterator<Entry<Long, ExtraRowStorage>> extraFiles = fileManager.getLRUExtraFiles();
		while (extraFiles.hasNext()) {
			ExtraRowStorage extraFile = extraFiles.next().getValue();
			while (extraFile.makeRoom()) {
				// Free up space until either enough is available or is nothing left to free up
				if ((maxSize - used) >= amount) {
					return true;
				}
			}
		}
		return false;
	}

	public long getSpaceUsed(SharedSpaceConsumer consumer) {
		Long spaceUsed = consumers.get(consumer);
		if (spaceUsed == null) {
			return 0;
		} else {
			return spaceUsed;
		}
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
//		System.out.println("Used after releaseAll: " + used);
	}

}
