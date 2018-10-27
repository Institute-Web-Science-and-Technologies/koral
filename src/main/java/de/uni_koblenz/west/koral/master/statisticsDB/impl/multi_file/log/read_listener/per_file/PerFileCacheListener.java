package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file;

import java.io.File;
import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.CompressedCSVWriter;

public class PerFileCacheListener {

	private final CompressedCSVWriter csvWriter;

	/**
	 * Recycled for each row to prevent millions of memory allocations
	 */
	private final Object[] values;

	private final boolean alignToGlobal;

	public PerFileCacheListener(int fileId, boolean alignToGlobal, String outputPath) {
		this.alignToGlobal = alignToGlobal;
		File outputFile = new File(outputPath,
				"cacheUsage_fileId" + fileId + (alignToGlobal ? "_globalAligned" : "") + ".csv.gz");
		csvWriter = new CompressedCSVWriter(outputFile, 1_000_000);
		csvWriter.printHeader("CACHE_USAGE", "FILE_SIZE", "PERCENTAGE_CACHED");

		values = new Object[3];
	}

	public void onLogRowRead(Map<String, Object> data, long globalRowCounter) {
		values[0] = data.get(StorageLogWriter.KEY_ACCESS_CACHEUSAGE);
		values[1] = data.get(StorageLogWriter.KEY_ACCESS_FILESIZE);
		values[2] = data.get(StorageLogWriter.KEY_ACCESS_PERCENTAGECACHED);
		if (alignToGlobal) {
			csvWriter.addRecord(globalRowCounter, values, null, null);
		} else {
			csvWriter.addRecord(values, null, null);
		}
	}

	public void close(long globalRowCounter) {
		if (alignToGlobal) {
			csvWriter.finish(globalRowCounter);
		}
		csvWriter.close();
	}

}
