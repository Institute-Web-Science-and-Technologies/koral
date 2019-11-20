package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogEvent;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogReadListener;

/**
 * Listens to chunk-switch events (when a new chunk of the input dataset is read).
 *
 * @author Philipp Töws
 *
 */
public class ChunkSwitchListener implements StorageLogReadListener {
	private final CSVPrinter csvPrinter;

	private long globalRowCounter;

	public ChunkSwitchListener(String outputPath) {
		File csvFile = new File(outputPath, "chunkSwitches.csv.gz");
		CSVFormat csvFileFormat = CSVFormat.RFC4180.withRecordSeparator('\n');
		try {
			csvPrinter = new CSVPrinter(
					new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(csvFile, false)), "UTF-8"),
					csvFileFormat);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void onLogRowRead(int rowType, Map<String, Object> data) {
		if ((rowType == StorageLogEvent.READWRITE.ordinal()) || (rowType == StorageLogEvent.BLOCKFLUSH.ordinal())) {
			globalRowCounter++;
		}
		if (rowType == StorageLogEvent.CHUNKSWITCH.ordinal()) {
			try {
				csvPrinter.printRecord(globalRowCounter);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void close() {
		try {
			csvPrinter.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
