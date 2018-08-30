package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener;

import java.io.File;
import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogEvent;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogReadListener;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;

public class PerFileListener implements StorageLogReadListener {

	private final CompressedCSVWriter csvWriter;

	private final int fileId;

	public PerFileListener(int fileId, String outputPath) {
		this.fileId = fileId;
		csvWriter = new CompressedCSVWriter(new File(outputPath, "cacheUsage_fileId" + fileId + ".csv.gz"));
		csvWriter.printHeader("CACHE_USAGE", "PERCENTAGE_CACHED");
	}

	@Override
	public void onLogRowRead(int rowType, Map<String, Object> data) {
		if (rowType == StorageLogEvent.READWRITE.ordinal()) {
			if ((Byte) data.get(StorageLogWriter.KEY_FILEID) == fileId) {
				csvWriter.addRecord(data.get(StorageLogWriter.KEY_ACCESS_CACHEUSAGE),
						data.get(StorageLogWriter.KEY_ACCESS_PERCENTAGECACHED));
			}
		}
	}

	@Override
	public void close() {
		csvWriter.close();
	}

}
