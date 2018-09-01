package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogEvent;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogReadListener;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;

public class ImplementationListener implements StorageLogReadListener {

	/**
	 * For each fileId, a boolean (in Byte-form) is stored that describes wether this file is currently stored as file
	 * (1) or in-memory (0).
	 */
	private final HashMap<Byte, Byte> implementations;

	private long fileImplementations;

	private long inMemoryImplementations;

	private final CompressedCSVWriter csvWriter;

	public ImplementationListener(String outputPath) {
		implementations = new HashMap<>();
		csvWriter = new CompressedCSVWriter(new File(outputPath, "implementations.csv.gz"));
		csvWriter.printHeader("FILE_IMPLEMENTATIONS", "INMEMORY_IMPLEMENTATIONS");
	}

	@Override
	public void onLogRowRead(int rowType, Map<String, Object> data) {
		if (rowType == StorageLogEvent.READWRITE.ordinal()) {
			byte newImpl = (byte) data.get(StorageLogWriter.KEY_ACCESS_FILESTORAGE);
			Byte oldImpl = implementations.put((byte) data.get(StorageLogWriter.KEY_FILEID), newImpl);

			// Check if this file appears for the first time
			if (oldImpl == null) {
				if (newImpl == 0) {
					inMemoryImplementations++;
				} else {
					fileImplementations++;
				}
			} else if (newImpl != oldImpl) {
				// Only update values if the implementation changed
				if (newImpl == 0) {
					fileImplementations--;
					inMemoryImplementations++;
				} else {
					inMemoryImplementations--;
					fileImplementations++;
				}
			}
			csvWriter.addSimpleRecord(fileImplementations, inMemoryImplementations);
		}
	}

	@Override
	public void close() {
		csvWriter.close();
	}

}
