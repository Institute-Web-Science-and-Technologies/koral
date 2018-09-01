package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener;

import java.io.File;
import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogEvent;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogReadListener;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;

public class PerFileListener implements StorageLogReadListener {

	private final CompressedCSVWriter csvWriter;

	private final int fileId;

	/**
	 * Recycled for each row to prevent millions of memory allocations
	 */
	private final Object[] values;

	private final byte[] binaryValues;

	private long rowCounter;

	private final boolean alignToGlobal;

	public PerFileListener(int fileId, boolean alignToGlobal, String outputPath) {
		this.fileId = fileId;
		this.alignToGlobal = alignToGlobal;
		csvWriter = new CompressedCSVWriter(new File(outputPath,
				"cacheUsage_fileId" + fileId + (alignToGlobal ? "_globalAligned" : "") + ".csv.gz"));
		csvWriter.printHeader("CACHE_USAGE", "PERCENTAGE_CACHED", "CACHE_HITRATE", "FOUND_RATE", "WRITE_RATE");

		values = new Object[2];
		binaryValues = new byte[3];
		rowCounter = 0;
	}

	@Override
	public void onLogRowRead(int rowType, Map<String, Object> data) {
		if (rowType == StorageLogEvent.READWRITE.ordinal()) {
			if ((Byte) data.get(StorageLogWriter.KEY_FILEID) == fileId) {
				values[0] = data.get(StorageLogWriter.KEY_ACCESS_CACHEUSAGE);
				values[1] = data.get(StorageLogWriter.KEY_ACCESS_PERCENTAGECACHED);
				binaryValues[0] = (byte) data.get(StorageLogWriter.KEY_ACCESS_CACHEHIT);
				binaryValues[1] = (byte) data.get(StorageLogWriter.KEY_ACCESS_FOUND);
				binaryValues[2] = (byte) data.get(StorageLogWriter.KEY_ACCESS_WRITE);
				if (alignToGlobal) {
					csvWriter.addRecord(rowCounter, values, binaryValues);
				} else {
					csvWriter.addRecord(values, binaryValues);
				}
			}
			// Count each read/write row/event for aligning to global x axis
			rowCounter++;
		}
	}

	@Override
	public void close() {
		if (alignToGlobal) {
			csvWriter.finish(rowCounter);
		}
		csvWriter.close();
	}

}
