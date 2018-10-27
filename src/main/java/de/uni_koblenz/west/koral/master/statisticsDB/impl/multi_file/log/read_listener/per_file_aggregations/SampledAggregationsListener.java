package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogEvent;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogReadListener;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;

public class SampledAggregationsListener implements StorageLogReadListener {

	private final Map<Byte, FileAggregator> files;

	private final long samplingInterval;

	private long globalRowCounter;

	private long globalAccumulationCounter;

	private final boolean alignToGlobal;

	private final String outputPath;

	public SampledAggregationsListener(long samplingInterval, boolean alignToGlobal, String outputPath) {
		this.samplingInterval = samplingInterval;
		this.alignToGlobal = alignToGlobal;
		this.outputPath = outputPath;

		files = new HashMap<>();
		globalRowCounter = 0;
	}

	@Override
	public void onLogRowRead(int rowType, Map<String, Object> data) {
		if (rowType == StorageLogEvent.READWRITE.ordinal()) {
			byte fileId = (byte) data.get(StorageLogWriter.KEY_FILEID);
			if (!files.containsKey(fileId)) {
				String fileName = "aggregations_fileId" + fileId + (alignToGlobal ? "_globalAligned" : "") + ".csv.gz";
				files.put(fileId, new FileAggregator(new File(outputPath, fileName), alignToGlobal));
			}
			files.get(fileId).accumulate(data);
			globalAccumulationCounter++;
			if (((globalRowCounter % samplingInterval) == 0) && (globalRowCounter > 0)) {
				for (FileAggregator file : files.values()) {
					file.printAggregations(globalRowCounter, globalAccumulationCounter);
				}
				globalAccumulationCounter = 0;
			}
			globalRowCounter++;
		}

	}

	@Override
	public void close() {
		files.values().forEach(file -> file.close(globalRowCounter));
	}

}
