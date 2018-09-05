package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogEvent;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogReadListener;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;

public class SampledAggregationsListener implements StorageLogReadListener {

	/**
	 * For each file there are two kind of accumulations collected: One where aggregations happen in perspective to this
	 * file only, and one for a global perspective. For example, Cache Hits are a percentage of accesses to this file
	 * only, while the file-access-activity metric describes a percentage in respect to all file accesses.
	 */
	private final Map<Byte, Long[][]> perFileAccumulations;

	private final Map<Byte, Long> perFileAccumulationCounter;

	private final Map<Byte, Long> perFileRowCounter;

	private final boolean alignToGlobal;

	private final long samplingInterval;

	private long globalRowCounter;

	private final Map<Byte, CompressedCSVWriter> perFileCsvWriter;

	private long globalAccumulationCounter;

	public SampledAggregationsListener(Set<Byte> fileIds, long samplingInterval, boolean alignToGlobal,
			String outputPath) {
		this.samplingInterval = samplingInterval;
		this.alignToGlobal = alignToGlobal;
		perFileAccumulations = new HashMap<>();
		perFileAccumulationCounter = new HashMap<>();
		perFileRowCounter = new HashMap<>();
		perFileCsvWriter = new HashMap<>();
		for (Byte fileId : fileIds) {
			Long[][] accArray = new Long[2][];
			accArray[0] = new Long[2];
			Arrays.fill(accArray[0], 0L);
			accArray[1] = new Long[1];
			Arrays.fill(accArray[1], 0L);
			perFileAccumulations.put(fileId, accArray);

			perFileAccumulationCounter.put(fileId, 0L);
			perFileRowCounter.put(fileId, 0L);

			String fileName = "aggregations_fileId" + fileId + (alignToGlobal ? "_globalAligned" : "") + ".csv.gz";
			CompressedCSVWriter csvWriter = new CompressedCSVWriter(new File(outputPath, fileName));
			csvWriter.printHeader("CACHE_HITRATE", "WRITE_RATE", "ACCESS_RATE");
			perFileCsvWriter.put(fileId, csvWriter);
		}
		globalRowCounter = 0;
	}

	@Override
	public void onLogRowRead(int rowType, Map<String, Object> data) {
		if (rowType == StorageLogEvent.READWRITE.ordinal()) {
			byte fileId = (byte) data.get(StorageLogWriter.KEY_FILEID);
			if (perFileAccumulations.containsKey(fileId)) {
				Long[][] accumulations = perFileAccumulations.get(fileId);
				// Accumulate metrics for file-local aggregation
				accumulations[0][0] += (Byte) data.get(StorageLogWriter.KEY_ACCESS_CACHEHIT);
				accumulations[0][1] += (Byte) data.get(StorageLogWriter.KEY_ACCESS_WRITE);
				// Accumulate metrics for global aggregation
				accumulations[1][0] += 1L;
				perFileAccumulationCounter.put(fileId, perFileAccumulationCounter.get(fileId) + 1);
				perFileRowCounter.put(fileId, perFileRowCounter.get(fileId) + 1);
				globalAccumulationCounter++;
			}
			if ((globalRowCounter % samplingInterval) == 0) {
				printAggregations(aggregate());
				resetAccumulations();
			}
			globalRowCounter++;
		}

	}

	private void resetAccumulations() {
		globalAccumulationCounter = 0;
		for (Entry<Byte, Long[][]> entry : perFileAccumulations.entrySet()) {
			for (Long[] array : entry.getValue()) {
				Arrays.fill(array, 0L);
			}
		}
		for (Byte fileId : perFileAccumulationCounter.keySet()) {
			perFileAccumulationCounter.put(fileId, 0L);
		}
	}

	private Map<Byte, Float[]> aggregate() {
		Map<Byte, Float[]> perFileAggregations = new HashMap<>();
		for (Entry<Byte, Long[][]> entry : perFileAccumulations.entrySet()) {
			Byte fileId = entry.getKey();
			Long[][] accumulations = entry.getValue();
			Long accumulationCounter = perFileAccumulationCounter.get(fileId);
			if (accumulationCounter == 0) {
				// No data was accumulated for this file
				continue;
			}
			Float[] aggregations = new Float[accumulations[0].length + accumulations[1].length];
			// Used for flattening 2D array
			int aggIndex = 0;
			// Aggregate metrics with file-local perspective
			for (int i = 0; i < accumulations[0].length; i++) {
				aggregations[aggIndex] = (accumulations[0][i] / (float) accumulationCounter) * 100;
				aggIndex++;
			}
			// Aggregate metrics with global perspective
			for (int i = 0; i < accumulations[1].length; i++) {
				aggregations[aggIndex] = (accumulations[1][i] / (float) globalAccumulationCounter) * 100;
				aggIndex++;
			}
			perFileAggregations.put(fileId, aggregations);
		}
		return perFileAggregations;
	}

	private void printAggregations(Map<Byte, Float[]> perFileAggregations) {
		for (Entry<Byte, Float[]> entry : perFileAggregations.entrySet()) {
			Byte fileId = entry.getKey();
			Float[] aggregations = entry.getValue();
			if (alignToGlobal) {
				perFileCsvWriter.get(fileId).addRecord(globalRowCounter, aggregations, null, null);
			} else {
				perFileCsvWriter.get(fileId).addRecord(perFileRowCounter.get(fileId), aggregations, null, null);
			}
		}
	}

	@Override
	public void close() {
		for (Entry<Byte, CompressedCSVWriter> entry : perFileCsvWriter.entrySet()) {
			Byte fileId = entry.getKey();
			CompressedCSVWriter csvWriter = entry.getValue();
			long lastRecordId;
			if (alignToGlobal) {
				lastRecordId = globalRowCounter;
			} else {
				lastRecordId = perFileRowCounter.get(fileId);
			}
			csvWriter.finish(lastRecordId);
			csvWriter.close();
		}
	}

}
