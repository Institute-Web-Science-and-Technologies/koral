package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.CSVWriter;

/**
 * Wraps
 * {@link de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.Aggregator}
 * by aggregating data for an individual file and a given perspective (local or global).
 *
 * @author Philipp Töws
 *
 */
public class FileAggregator {

	private final Map<AggregatorType, Aggregator> aggregators;

	private long rowCounter;

	private final CSVWriter csvWriter;

	/**
	 * Holds the values of the enum AggregatorType in the same order as the headers will be printed.
	 */
	private final AggregatorType[] aggregatorOrder;

	private final boolean alignToGlobal;

	enum AggregatorType {
		FILE_LOCAL_PERCENTAGE,
		GLOBAL_PERCENTAGE,
		FILE_LOCAL_AVERAGE
	}

	public FileAggregator(File outputFile, boolean alignToGlobal, long firstRecordId) {
		this.alignToGlobal = alignToGlobal;
		aggregators = new HashMap<>();
		csvWriter = new CSVWriter(outputFile, firstRecordId);
		rowCounter = 0;

		// If any of the following three structures is changed, the others most probably have to be updated as well
		csvWriter.printHeader("CACHE_HITRATE", "WRITE_RATE", "ACCESS_RATE", "TIME_AVG_CACHE_READ",
				"TIME_AVG_CACHE_WRITE", "TIME_AVG_FILE_READ", "TIME_AVG_FILE_WRITE", "TIME_AVG_TOTAL");
		aggregatorOrder = new AggregatorType[] {
				AggregatorType.FILE_LOCAL_PERCENTAGE,
				AggregatorType.GLOBAL_PERCENTAGE,
				AggregatorType.FILE_LOCAL_AVERAGE };
		aggregators.put(AggregatorType.FILE_LOCAL_PERCENTAGE, new Aggregator(2) {
			@Override
			protected float aggregate(long accumulatedValue, long accumulationCounter, long extraValue) {
				return (accumulatedValue / (float) accumulationCounter) * 100;
			}
		});

		aggregators.put(AggregatorType.GLOBAL_PERCENTAGE, new Aggregator(1) {
			@Override
			protected float aggregate(long accumulatedValue, long accumulationCounter, long extraValue) {
				// extraValue is supposed to be globalRowCounter
				return (accumulatedValue / (float) extraValue) * 100;
			}
		});

		aggregators.put(AggregatorType.FILE_LOCAL_AVERAGE, new Aggregator(5) {
			@Override
			protected float aggregate(long accumulatedValue, long accumulationCounter, long extraValue) {
				return accumulatedValue / (float) accumulationCounter;
			}
		});
	}

	public FileAggregator(File outputFile, boolean alignToGlobal) {
		this(outputFile, alignToGlobal, 0);
	}

	/**
	 * Extracts the data for the metrics from the event data and accumulates them in their corresponding aggregator.
	 *
	 * @param data
	 *            The event data that maps from data identifiers (as defined in StorageLogWriter) to their values.
	 */
	public void accumulate(Map<String, Object> data) {
		aggregators.get(AggregatorType.FILE_LOCAL_PERCENTAGE).accumulate(
				(byte) data.get(StorageLogWriter.KEY_ACCESS_CACHEHIT),
				(byte) data.get(StorageLogWriter.KEY_ACCESS_WRITE));
		// Accumulate one per file access for access metric
		aggregators.get(AggregatorType.GLOBAL_PERCENTAGE).accumulate(1);
		Long[] timeColumns = new Long[5];
		boolean cacheAccess = (byte) data.get(StorageLogWriter.KEY_ACCESS_CACHEHIT) > 0;
		boolean write = (byte) data.get(StorageLogWriter.KEY_ACCESS_WRITE) > 0;
		// Choose column based on header order
		int column = 0;
		if (cacheAccess && !write) {
			column = 0;
		} else if (cacheAccess && write) {
			column = 1;
		} else if (!cacheAccess && !write) {
			column = 2;
		} else if (!cacheAccess && write) {
			column = 3;
		}
		timeColumns[column] = (Long) data.get(StorageLogWriter.KEY_ACCESS_TIME);
		timeColumns[4] = (Long) data.get(StorageLogWriter.KEY_ACCESS_TIME);
		aggregators.get(AggregatorType.FILE_LOCAL_AVERAGE).accumulate(timeColumns);
		rowCounter++;
	}

	/**
	 * Triggers aggregation for each aggregator and writes the results into the output csv file.
	 *
	 * @param globalRowCounter
	 *            The amount of all events that have been collected until now.
	 * @param globalAccumulationCounter
	 *            The amount of events that have been collected since the last aggregation.
	 */
	public void printAggregations(long globalRowCounter, long globalAccumulationCounter) {
		Float[] aggregations = aggregate(globalAccumulationCounter);
		if (alignToGlobal) {
			csvWriter.addIntervalRecord(globalRowCounter - 1, (Object[]) aggregations);
		} else {
			csvWriter.addIntervalRecord(rowCounter - 1, (Object[]) aggregations);
		}
	}

	private Float[] aggregate(long globalAccumulationCounter) {
		List<Float[]> aggregatedArrays = new LinkedList<>();
		for (AggregatorType aggType : aggregatorOrder) {
			aggregatedArrays.add(aggregators.get(aggType).aggregate(globalAccumulationCounter));
		}
		int arraySize = 0;
		for (Float[] array : aggregatedArrays) {
			arraySize += array.length;
		}
		Float[] allAggregations = new Float[arraySize];
		int arrayIndex = 0;
		for (Float[] array : aggregatedArrays) {
			System.arraycopy(array, 0, allAggregations, arrayIndex, array.length);
			arrayIndex += array.length;
		}
		aggregators.values().forEach(agg -> agg.reset());
		return allAggregations;
	}

	public void close() {
		csvWriter.close();
	}

}
