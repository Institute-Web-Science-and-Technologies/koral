package de.uni_koblenz.west.cidre.master.statisticsDB;

import java.io.Closeable;
import java.util.Arrays;
import java.util.BitSet;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.master.statisticsDB.impl.MapDBGraphStatisticsDatabase;

public class GraphStatistics implements Closeable {

	@SuppressWarnings("unused")
	private final Logger logger;

	private final GraphStatisticsDatabase database;

	private final int numberOfChunks;

	public GraphStatistics(Configuration conf, short numberOfChunks,
			Logger logger) {
		this.logger = logger;
		this.numberOfChunks = numberOfChunks;
		database = new MapDBGraphStatisticsDatabase(
				conf.getStatisticsStorageType(),
				conf.getStatisticsDataStructure(), conf.getStatisticsDir(),
				conf.useTransactionsForStatistics(),
				conf.areStatisticsAsynchronouslyWritten(),
				conf.getStatisticsCacheType(), numberOfChunks);
	}

	public void count(long subject, long property, long object, int chunk) {
		database.incrementSubjectCount(subject, chunk);
		database.incrementRessourceOccurrences(subject, chunk);
		database.incrementPropertyCount(property, chunk);
		database.incrementRessourceOccurrences(property, chunk);
		database.incrementObjectCount(object, chunk);
		database.incrementRessourceOccurrences(object, chunk);
		database.incrementNumberOfTriplesPerChunk(chunk);
	}

	public long[] getChunkSizes() {
		return database.getChunkSizes();
	}

	public short getOwner(long id) {
		long[] statistics = database.getStatisticsForResource(id);
		// TODO remove
		System.out.println(">>>> " + id + " -> " + (statistics == null
				? statistics : Arrays.toString(statistics)));

		BitSet ownerCandidates = new BitSet(numberOfChunks);
		ownerCandidates.set(0, numberOfChunks, true);

		// check occurrence as subject
		argMax(statistics, 0 * numberOfChunks, ownerCandidates);
		assert ownerCandidates.size() > 0;
		if (ownerCandidates.size() == 1) {
			return (short) ownerCandidates.nextSetBit(0);
		}

		// check occurrence as object
		argMax(statistics, 2 * numberOfChunks, ownerCandidates);
		assert ownerCandidates.size() > 0;
		if (ownerCandidates.size() == 1) {
			return (short) ownerCandidates.nextSetBit(0);
		}

		// check occurrence as property
		argMax(statistics, 1 * numberOfChunks, ownerCandidates);
		assert ownerCandidates.size() > 0;
		if (ownerCandidates.size() == 1) {
			return (short) ownerCandidates.nextSetBit(0);
		}

		// check load of the different chunks
		statistics = database.getOwnerLoad();
		argMin(statistics, 0, ownerCandidates);
		assert ownerCandidates.size() > 0;
		return (short) ownerCandidates.nextSetBit(0);
	}

	private void argMax(long[] statistics, int offset, BitSet ownerCandidates) {
		long max = Long.MIN_VALUE;
		int currentPos = ownerCandidates.nextSetBit(0);
		while (currentPos > -1) {
			if (statistics[offset + currentPos] < max) {
				ownerCandidates.clear(currentPos);
			} else if (statistics[offset + currentPos] > max) {
				max = statistics[offset + currentPos];
				ownerCandidates.set(0, currentPos, false);
			}
			currentPos = ownerCandidates.nextSetBit(currentPos + 1);
		}
	}

	private void argMin(long[] statistics, int offset, BitSet ownerCandidates) {
		long min = Long.MAX_VALUE;
		int currentPos = ownerCandidates.nextSetBit(0);
		while (currentPos > -1) {
			if (statistics[offset + currentPos] > min) {
				ownerCandidates.clear(currentPos);
			} else if (statistics[offset + currentPos] < min) {
				min = statistics[offset + currentPos];
				ownerCandidates.set(0, currentPos, false);
			}
			currentPos = ownerCandidates.nextSetBit(currentPos + 1);
		}
	}

	/**
	 * updates the dictionary such that the first two bytes of id is set to
	 * owner.
	 * 
	 * @param id
	 * @param owner
	 * @return
	 * @throws IllegalArgumentException
	 *             if the first two bytes of id are not 0 or not equal to owner
	 */
	public long setOwner(long id, short owner) {
		return database.setOwner(id, owner);
	}

	public void clear() {
		database.clear();
	}

	@Override
	public void close() {
		database.close();
	}

}
