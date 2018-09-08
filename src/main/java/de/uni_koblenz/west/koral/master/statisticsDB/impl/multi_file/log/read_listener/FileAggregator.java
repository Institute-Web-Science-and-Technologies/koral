package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;

public class FileAggregator {

	private final Map<AggregatorType, Aggregator> aggregators;

	private long rowCounter;

	private final CompressedCSVWriter csvWriter;

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

	public FileAggregator(File outputFile, boolean alignToGlobal) {
		this.alignToGlobal = alignToGlobal;
		aggregators = new HashMap<>();
		csvWriter = new CompressedCSVWriter(outputFile);
		rowCounter = 0;

		// If any of the following three structures is changed, the others most probably have to be updated as well
		csvWriter.printHeader("CACHE_HITRATE", "WRITE_RATE", "ACCESS_RATE", "TIME_AVG");
		aggregatorOrder = new AggregatorType[] {
				AggregatorType.FILE_LOCAL_PERCENTAGE,
				AggregatorType.GLOBAL_PERCENTAGE,
				AggregatorType.FILE_LOCAL_AVERAGE };
		aggregators.put(AggregatorType.FILE_LOCAL_PERCENTAGE, new Aggregator(2) {
			@Override
			protected float aggregate(long accumulatedValue, long extraValue) {
				return (accumulatedValue / (float) accumulationCounter) * 100;
			}
		});

		aggregators.put(AggregatorType.GLOBAL_PERCENTAGE, new Aggregator(1) {
			@Override
			protected float aggregate(long accumulatedValue, long extraValue) {
				return (accumulatedValue / (float) extraValue) * 100;
			}
		});

		aggregators.put(AggregatorType.FILE_LOCAL_AVERAGE, new Aggregator(1) {
			@Override
			protected float aggregate(long accumulatedValue, long extraValue) {
				return accumulatedValue / (float) accumulationCounter;
			}
		});
	}

	public void accumulate(Map<String, Object> data) {
		aggregators.get(AggregatorType.FILE_LOCAL_PERCENTAGE).accumulate(
				(byte) data.get(StorageLogWriter.KEY_ACCESS_CACHEHIT),
				(byte) data.get(StorageLogWriter.KEY_ACCESS_WRITE));
		// Accumulate one per file access for access metric
		aggregators.get(AggregatorType.GLOBAL_PERCENTAGE).accumulate(1);
		aggregators.get(AggregatorType.FILE_LOCAL_AVERAGE).accumulate(
				(int) data.get(StorageLogWriter.KEY_ACCESS_TIME));
		rowCounter++;
	}

	public void printAggregations(long globalRowCounter, long globalAccumulationCounter) {
		Float[] aggregations = aggregate(globalAccumulationCounter);
		if (alignToGlobal) {
			csvWriter.addRecord(globalRowCounter, aggregations, null, null);
		} else {
			csvWriter.addRecord(rowCounter, aggregations, null, null);
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

	public void close(long globalRowCounter) {
		long lastRecordId;
		if (alignToGlobal) {
			lastRecordId = globalRowCounter;
		} else {
			lastRecordId = rowCounter;
		}
		csvWriter.finish(lastRecordId);
		csvWriter.close();
	}

}
