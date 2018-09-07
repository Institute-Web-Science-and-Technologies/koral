package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogEvent;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogReadListener;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;

public class SampledAggregationsListener implements StorageLogReadListener {

	private final Map<Byte, FileAggregator> files;

	private final long samplingInterval;

	private long globalRowCounter;

	private long globalAccumulationCounter;

	public SampledAggregationsListener(Set<Byte> fileIds, long samplingInterval, boolean alignToGlobal,
			String outputPath) {
		this.samplingInterval = samplingInterval;

		files = new HashMap<>();
		for (Byte fileId : fileIds) {
			String fileName = "aggregations_fileId" + fileId + (alignToGlobal ? "_globalAligned" : "") + ".csv.gz";
			files.put(fileId, new FileAggregator(new File(outputPath, fileName), alignToGlobal));
		}
		globalRowCounter = 0;
	}

	@Override
	public void onLogRowRead(int rowType, Map<String, Object> data) {
		if (rowType == StorageLogEvent.READWRITE.ordinal()) {
			byte fileId = (byte) data.get(StorageLogWriter.KEY_FILEID);
			if (files.containsKey(fileId)) {
				files.get(fileId).accumulate(data);
				globalAccumulationCounter++;
			}
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
