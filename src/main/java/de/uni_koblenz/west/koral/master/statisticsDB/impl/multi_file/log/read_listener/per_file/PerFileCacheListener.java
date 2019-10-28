package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file;

import java.io.File;
import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.CSVWriter;

/**
 * Accumulates cache-related values for a single file.
 *
 * @author Philipp Töws
 *
 */
public class PerFileCacheListener {

	private final CSVWriter csvWriter;

	/**
	 * Recycled for each row to prevent millions of memory allocations
	 */
	private final Object[] values;

	private final boolean alignToGlobal;

	public PerFileCacheListener(int fileId, boolean alignToGlobal, String outputPath) {
		this.alignToGlobal = alignToGlobal;
		File outputFile = new File(outputPath,
				"cacheUsage_fileId" + fileId + (alignToGlobal ? "_globalAligned" : "") + ".csv.gz");
		csvWriter = new CSVWriter(outputFile);
		csvWriter.printHeader("CACHE_USAGE", "FILE_SIZE", "PERCENTAGE_CACHED");

		values = new Object[3];
	}

	public void onLogRowRead(Map<String, Object> data, long globalRowCounter) {
		values[0] = data.get(StorageLogWriter.KEY_CACHEUSAGE);
		values[1] = data.get(StorageLogWriter.KEY_ACCESS_FILESIZE);
		values[2] = data.get(StorageLogWriter.KEY_PERCENTAGECACHED);
		if (alignToGlobal) {
			csvWriter.addRecord(globalRowCounter, values);
		} else {
			csvWriter.addRecord(values);
		}
	}

	public void close(long globalRowCounter) {
		if (alignToGlobal) {
//			csvWriter.finish(globalRowCounter);
		}
		csvWriter.close();
	}

}
