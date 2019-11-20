package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file;

import java.io.File;
import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.CSVWriter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.Aggregator;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.aggregations.FileLocalPercentageAggregator;

/**
 * Listens to the BlockFlush events and logs the rate of blocks that were flushed and had the dirty flag set.
 */
public class PerFileBlockFlushListener {

	private final boolean alignToGlobal;
	private final long intervalLength;

	private final Aggregator aggregator;

	private final CSVWriter csvWriter;

	private long rowCounter;
	private long lastGlobalRowCount;

	public PerFileBlockFlushListener(byte fileId, long intervalLength, boolean alignToGlobal, String outputPath) {
		this.intervalLength = intervalLength;
		this.alignToGlobal = alignToGlobal;

		File csvFile = new File(outputPath,
				"blockFlushes_fileId" + fileId + (alignToGlobal ? "_globalAligned" : "") + ".csv.gz");
		csvWriter = new CSVWriter(csvFile);
		csvWriter.printHeader(alignToGlobal ? "GLOBAL_ROW" : "LOCAL_ROW", "PERCENTAGE_DIRTY");

		aggregator = new FileLocalPercentageAggregator();
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
		Float dirtyRate = aggregator.aggregate();
		aggregator.reset();
		if (!alignToGlobal) {
			csvWriter.addIntervalRecord(rowCounter, dirtyRate);
		} else {
			csvWriter.addIntervalRecord(globalRowCounter, dirtyRate);
		}
	}

	public void close(long globalRowCounter) {
		writeInterval(globalRowCounter);
		csvWriter.close();
	}

}
