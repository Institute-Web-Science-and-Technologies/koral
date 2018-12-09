package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.shared_space;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.CentralLogger;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.FileManager;

/**
 * A SharedSpaceManager that makes room by requesting the consumer with the highest access based shares exceedence
 * (HABSE) to make room. The access based shares are computed by managing an access list/history over the last N
 * entries, and giving each file a proportional part of the space. These shares are stored in a table. When space is
 * requested, the consumer with the highest absolute exceedings of his shares is asked to make room.
 *
 * @param fileManager
 * @param maxSize
 */
public class HABSESharedSpaceManager extends SharedSpaceManager {

	/**
	 * The history list of the consumers. Has length {@link #historyLength}. Front element is most recently accessed
	 * consumer.
	 */
	private final LinkedList<SharedSpaceConsumer> accessHistory;

	/**
	 * The access count table. Contains always updated occurence frequencies of consumers in {@link #accessHistory}.
	 * Represents the shares each consumer is allowed to use.
	 */
	private final HashMap<SharedSpaceConsumer, Long> recentAccessCount;

	/**
	 * The length of {@link #accessHistory}, i.e. how many recent accesses are considered for distributing the shared
	 * space.
	 */
	private final int historyLength;

	/**
	 *
	 * @param fileManager
	 * @param maxSize
	 *            The total available size of space this manager will manage
	 * @param historyLength
	 *            The length of the access history list
	 */
	public HABSESharedSpaceManager(FileManager fileManager, long maxSize, int historyLength) {
		super(fileManager, maxSize);
		this.historyLength = historyLength;

		accessHistory = new LinkedList<>();
		recentAccessCount = new HashMap<>();
	}

	/**
	 * Finds the consumer that currently has the highest access based share exceedence (HABSE), i.e. the largest
	 * absolute difference of used to allowed cache share.
	 *
	 * @return
	 */
	private SharedSpaceConsumer findHABSE() {
		long start = System.nanoTime();
		double maxExceedence = 0;
		SharedSpaceConsumer habseConsumer = null;
		long totalAccessCosts = 0;
		for (SharedSpaceConsumer c : recentAccessCount.keySet()) {
			totalAccessCosts += c.accessCosts();
		}
		for (Entry<SharedSpaceConsumer, Long> e : recentAccessCount.entrySet()) {
			SharedSpaceConsumer consumer = e.getKey();
			long recentAccesses = e.getValue();

			double accessesShare = recentAccesses / (double) historyLength;
			double accessCostsShare = consumer.accessCosts() / (double) totalAccessCosts;
			double allowedShare = (0.3 * accessesShare) + (0.7 * accessCostsShare);

			double usedCacheShare = getSpaceUsed(consumer) / (double) maxSize;

			double exceedence = usedCacheShare - allowedShare;
			// Greater or equal comparison because it might be possible that each file uses exactly its allowed share,
			// resulting in zero exceedences only
			if (exceedence >= maxExceedence) {
				maxExceedence = exceedence;
				habseConsumer = consumer;
			}
		}
		long time = System.nanoTime() - start;
		CentralLogger.getInstance().addHABSETime(time);
		return habseConsumer;
	}

	/**
	 * Asks each consumer in order of HABSE to make room until enough space is available.
	 */
	@Override
	protected boolean makeRoom(SharedSpaceConsumer requester, long amount) {
		for (SharedSpaceConsumer consumer; (consumer = findHABSE()) != null;) {
			if ((consumer == requester) && !consumer.isAbleToMakeRoomForOwnRequests()) {
				return false;
			}
			while (consumer.makeRoom()) {
				// Free up space until either enough is available or nothing is left to free up
				if ((maxSize - used) >= amount) {
					return true;
				}
			}
		}
		throw new RuntimeException("Could not find any more HABSE consumers");
	}

	/**
	 * Updates internal access history and share table with this new access.
	 *
	 * @param consumer
	 */
	public void notifyAccess(SharedSpaceConsumer consumer) {
		if (consumer == null) {
			throw new NullPointerException("Consumer cannot be null");
		}
		SharedSpaceConsumer last = null;
		assert accessHistory.size() <= historyLength;
		if (accessHistory.size() == historyLength) {
			last = accessHistory.pollLast();
		}
		accessHistory.addFirst(consumer);
		assert accessHistory.size() <= historyLength;

		// Update shares
		if (last != null) {
			Long lastAccessCount = recentAccessCount.get(last);
			if (lastAccessCount == null) {
				throw new IllegalStateException("Cannot remove unlisted consumer from access list");
			}
			if (lastAccessCount == 0) {
				throw new IllegalStateException("Cannot remove consumer with zero recent accesses");
			}
			recentAccessCount.put(last, lastAccessCount - 1);
		}
		Long consumerAccessCount = recentAccessCount.get(consumer);
		if (consumerAccessCount == null) {
			consumerAccessCount = 0L;
		}
		recentAccessCount.put(consumer, consumerAccessCount + 1);

	}

}
