package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogEvent;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogReadListener;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;

public class ExtraCacheUsageListener implements StorageLogReadListener {

	private final HashMap<Byte, Integer> cacheUsages;

	private final CompressedCSVWriter csvWriter;

	public ExtraCacheUsageListener(String outputPath) {
		cacheUsages = new HashMap<>();
		csvWriter = new CompressedCSVWriter(new File(outputPath, "extraCacheUsage.csv.gz"));
	}

	@Override
	public void onLogRowRead(int rowType, Map<String, Object> data) {
		if (rowType == StorageLogEvent.READWRITE.ordinal()) {
			if ((Byte) data.get(StorageLogWriter.KEY_FILEID) > 0) {
				cacheUsages.put((byte) data.get(StorageLogWriter.KEY_FILEID),
						(int) data.get(StorageLogWriter.KEY_ACCESS_CACHEUSAGE));
				long totalCacheUsage = cacheUsages.values().stream().reduce(0, (sum, size) -> sum + size);
				csvWriter.print(totalCacheUsage);
			}
		}
	}

	@Override
	public void close() {
		csvWriter.close();
	}

}
