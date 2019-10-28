package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogEvent;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogReadListener;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;

/**
 * Collects the amount of used implementations, i.e. how many files used in-memory and
 * random-access-file-implementations.
 *
 * @author Philipp Töws
 *
 */
public class ImplementationListener implements StorageLogReadListener {

	/**
	 * For each fileId, a boolean (in Byte-form) is stored that describes wether this file is currently stored as file
	 * (1) or in-memory (0).
	 */
	private final HashMap<Byte, Byte> implementations;

	private long fileImplementations;

	private long inMemoryImplementations;

	private long globalRowCounter;

	private final CSVWriter csvWriter;

	public ImplementationListener(String outputPath) {
		implementations = new HashMap<>();
		csvWriter = new CSVWriter(new File(outputPath, "implementations.csv.gz"));
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
				csvWriter.addIntervalRecord(globalRowCounter, fileImplementations, inMemoryImplementations);
			} else if (newImpl != oldImpl) {
				// Only update values if the implementation changed
				if (newImpl == 0) {
					fileImplementations--;
					inMemoryImplementations++;
				} else {
					inMemoryImplementations--;
					fileImplementations++;
				}
				csvWriter.addIntervalRecord(globalRowCounter, fileImplementations, inMemoryImplementations);
			}
			globalRowCounter++;
		}
	}

	@Override
	public void close() {
		csvWriter.addIntervalRecord(globalRowCounter, fileImplementations, inMemoryImplementations);
		csvWriter.close();
	}

}
