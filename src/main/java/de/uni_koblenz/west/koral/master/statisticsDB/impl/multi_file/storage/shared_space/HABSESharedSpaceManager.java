package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.shared_space;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.collections4.queue.CircularFifoQueue;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.FileManager;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.SubbenchmarkManager;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.SubbenchmarkManager.SUBBENCHMARK_TASK;
import playground.StatisticsDBTest;

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
	private final CircularFifoQueue<SharedSpaceConsumer> accessHistory;

	/**
	 * The access count table. Contains always updated occurence frequencies of consumers in {@link #accessHistory}.
	 * Represents the shares each consumer is allowed to use.
	 */
	private final HashMap<SharedSpaceConsumer, Long> recentAccessCount;

	/**
	 * The length of {@link #accessHistory}, i.e. how many recent accesses are considered for distributing the shared
	 * space.
	 */
	private final long historyLength;

	private final float accessesWeight;

	private SharedSpaceConsumer[] recyclableConsumerArray;
	private double[] recyclableExceedencesArray;

	/**
	 *
	 * @param fileManager
	 * @param maxSize
	 *            The total available size of space this manager will manage
	 * @param accessesWeight
	 *            Value between [0,1]. Determines how big of a role the recent accesses of the file play when
	 *            calculating the allowed cache shares.
	 * @param historyLength
	 *            The length of the access history list
	 */
	public HABSESharedSpaceManager(FileManager fileManager, long maxSize, float accessesWeight, int historyLength) {
		super(fileManager, maxSize);
		this.accessesWeight = accessesWeight;
		this.historyLength = historyLength;

		accessHistory = new CircularFifoQueue<>(historyLength);
		recentAccessCount = new HashMap<>();
	}

	/**
	 * Lazily returns the consumers sorted by their highest access based share exceedence (HABSE), i.e. the largest
	 * absolute difference of used to allowed cache share.
	 *
	 * @return
	 */
	private Iterator<SharedSpaceConsumer> findHABSE() {
		long start = 0;
		if (StatisticsDBTest.SUBBENCHMARKS) {
			start = System.nanoTime();
		}

		long totalAccessCosts = 0;
		if (accessesWeight < 1) {
			for (SharedSpaceConsumer c : recentAccessCount.keySet()) {
				totalAccessCosts += c.accessCosts();
			}
		}

		SharedSpaceConsumer[] consumers;
		double[] exceedences;
		int consumerCount = recentAccessCount.keySet().size();
		if ((recyclableConsumerArray != null) && (recyclableConsumerArray.length == consumerCount)) {
			consumers = recyclableConsumerArray;
		} else {
			consumers = new SharedSpaceConsumer[consumerCount];
			recyclableConsumerArray = consumers;
		}
		if ((recyclableExceedencesArray != null) && (recyclableExceedencesArray.length == consumerCount)) {
			exceedences = recyclableExceedencesArray;
		} else {
			exceedences = new double[consumerCount];
			recyclableExceedencesArray = exceedences;
		}

		// Generate two arrays with all the consumers and their respective exceedence value for use in the iterator
		int i = 0;
		for (Entry<SharedSpaceConsumer, Long> e : recentAccessCount.entrySet()) {
			SharedSpaceConsumer consumer = e.getKey();
			long recentAccesses = e.getValue();

			double accessesShare = recentAccesses / (double) historyLength;
			double accessCostsShare = 0;
			if (accessesWeight < 1) {
				accessCostsShare = consumer.accessCosts() / (double) totalAccessCosts;
			}
			double allowedShare = (accessesWeight * accessesShare) + ((1 - accessesWeight) * accessCostsShare);

			double usedCacheShare = getSpaceUsed(consumer) / (double) maxSize;

			double exceedence = usedCacheShare - allowedShare;

			consumers[i] = consumer;
			exceedences[i] = exceedence;
			i++;
		}
		if (StatisticsDBTest.SUBBENCHMARKS) {
			SubbenchmarkManager.getInstance().addTime(SUBBENCHMARK_TASK.HABSE_FIND, System.nanoTime() - start);
		}

		return new Iterator<SharedSpaceConsumer>() {

			private SharedSpaceConsumer nextHabseConsumer = findNext();

			private SharedSpaceConsumer findNext() {
				int maxIndex = -1;
				// Minimum exceedence is -1, because it is the difference between two 0..1 ratios
				double maxExceedence = -1;
				SharedSpaceConsumer habseConsumer = null;
				for (int i = 0; i < consumers.length; i++) {
					// Also note that it is impossible to have a consumer here that owns no space, because then it would
					// not be in the recentAccessCount map.
					if ((exceedences[i] >= maxExceedence)) {
						maxExceedence = exceedences[i];
						habseConsumer = consumers[i];
						maxIndex = i;
					}
				}
				if (maxIndex >= 0) {
					// Mark found maximum to exclude from next search
					exceedences[maxIndex] = -1;
				}
				return habseConsumer;
			}

			@Override
			public boolean hasNext() {
				return nextHabseConsumer != null;
			}

			@Override
			public SharedSpaceConsumer next() {
				SharedSpaceConsumer next = nextHabseConsumer;
				nextHabseConsumer = findNext();
				return next;
			}
		};
	}

	/**
	 * Asks each consumer in order of HABSE to make room until enough space is available.
	 */
	@Override
	protected boolean makeRoom(SharedSpaceConsumer requester, long amount) {
		SharedSpaceConsumer lastHabse = null;
		int hangCounter = 0;
		Iterator<SharedSpaceConsumer> habses = findHABSE();
		while (habses.hasNext()) {
			SharedSpaceConsumer consumer = habses.next();
			if (lastHabse == consumer) {
				hangCounter++;
				System.out.println("Hanging at consumer " + consumer);
//				throw new RuntimeException("Hanging at same Habse consumer");
			}
			if ((consumer == requester) && !consumer.isAbleToMakeRoomForOwnRequests()) {
				// If isAbleToMakeRoomForOwnRequests() returns false, we cannot call makeRoom() on this consumer.
				// We return false, so the requester can then still take measures to make room, e.g. by switching
				// to a different implementation.
				if (hangCounter > 0) {
					System.out.println("Hanged for " + hangCounter + " iterations");
				}
				return false;
			}
			while (consumer.makeRoom()) {
				// Free up space until either enough is available or nothing is left to free up
				if ((maxSize - used) >= amount) {
					if (hangCounter > 0) {
						System.out.println("Hanged for " + hangCounter + " iterations");
					}
					return true;
				}
			}
			lastHabse = consumer;
		}
		throw new RuntimeException("Could not find any more HABSE consumers");
	}

	/**
	 * Updates internal access history and share table with this new access.
	 *
	 * @param consumer
	 */
	public void notifyAccess(SharedSpaceConsumer consumer) {
		long start = 0;
		if (StatisticsDBTest.SUBBENCHMARKS) {
			start = System.nanoTime();
		}
		if (consumer == null) {
			throw new NullPointerException("Consumer cannot be null");
		}
		SharedSpaceConsumer last = null;
		assert accessHistory.size() <= historyLength;
		if (accessHistory.size() == historyLength) {
			last = accessHistory.poll();
		}
		assert accessHistory.size() < historyLength;
		accessHistory.add(consumer);
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
		if (StatisticsDBTest.SUBBENCHMARKS) {
			SubbenchmarkManager.getInstance().addTime(SubbenchmarkManager.SUBBENCHMARK_TASK.HABSE_NOTIFY_ACCESS,
					System.nanoTime() - start);
		}
	}

}
