package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file;

import java.io.File;
import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.CompressedCSVWriter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.Aggregator;

public class PerFileBlockFlushListener {

	private final boolean alignToGlobal;
	private final String outputPath;
	private final byte fileId;
	private final long intervalLength;

	private final Aggregator aggregator;

	private final CompressedCSVWriter csvWriter;

	private long rowCounter;
	private long lastGlobalRowCount;

	public PerFileBlockFlushListener(byte fileId, long intervalLength, boolean alignToGlobal, String outputPath) {
		this.fileId = fileId;
		this.intervalLength = intervalLength;
		this.alignToGlobal = alignToGlobal;
		this.outputPath = outputPath;

		File csvFile = new File(outputPath,
				"blockFlushes_fileId" + fileId + (alignToGlobal ? "_globalAligned" : "") + ".csv.gz");
		csvWriter = new CompressedCSVWriter(csvFile);
		csvWriter.printHeader(alignToGlobal ? "GLOBAL_ROW" : "LOCAL_ROW", "PERCENTAGE_DIRTY");

		aggregator = new Aggregator(1) {
			@Override
			protected float aggregate(long accumulatedValue, long accumulationCounter, long extraValue) {
				return (accumulatedValue / (float) accumulationCounter) * 100;
			}
		};
	}

	public void onLogRowRead(Map<String, Object> data, long globalRowCounter) {
		byte dirty = (byte) data.get(StorageLogWriter.KEY_BLOCKFLUSH_DIRTY);
		aggregator.accumulate(dirty);
		if (!alignToGlobal) {
			if ((rowCounter > 0) && ((rowCounter % intervalLength) == 0)) {
				writeInterval(globalRowCounter);
			}
		} else {
			if ((globalRowCounter - lastGlobalRowCount) > intervalLength) {
				writeInterval(globalRowCounter);
				lastGlobalRowCount = globalRowCounter;
			}
		}
		rowCounter++;
	}

	private void writeInterval(long globalRowCounter) {
		Float dirtyRate = aggregator.aggregate()[0];
		aggregator.reset();
		// Wrap in array for var args handling
		csvWriter.addSimpleRecord(new Object[] { dirtyRate });
	}

	public void close(long globalRowCounter) {
		writeInterval(globalRowCounter);
		csvWriter.close();
	}

}
