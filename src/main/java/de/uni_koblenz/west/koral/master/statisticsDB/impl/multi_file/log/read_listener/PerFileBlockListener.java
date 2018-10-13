package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.counter.PersistantCounter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.counter.impl.RocksDBStore;

public class PerFileBlockListener {

	private final boolean alignToGlobal;
	private final String outputPath;

	private long rowCounter;
	private final long intervalLength;
	private long maxBlockId;

	/**
	 * Counts block accesses for each interval. Is cleared after each interval and reused.
	 */
	private PersistantCounter counter;
	private final int maxOpenFiles;

	private final CSVPrinter csvWriter;
	private final byte fileId;

	public PerFileBlockListener(byte fileId, long intervalLength, int maxOpenFiles, boolean alignToGlobal,
			String outputPath) {
		this.fileId = fileId;
		this.intervalLength = intervalLength;
		this.maxOpenFiles = maxOpenFiles;
		this.alignToGlobal = alignToGlobal;
		this.outputPath = outputPath;

		File csvFile = new File(outputPath,
				"blockAccesses_fileId" + fileId + (alignToGlobal ? "_globalAligned" : "") + ".csv.gz");
		CSVFormat csvFileFormat = CSVFormat.RFC4180.withRecordSeparator('\n');
		try {
			csvWriter = new CSVPrinter(
					new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(csvFile, false)), "UTF-8"),
					csvFileFormat);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		newCounter();
	}

	public void onLogRowRead(Map<String, Object> data, long globalRowCounter) {
		long blockId = ((Number) data.get(StorageLogWriter.KEY_POSITION)).longValue();
		if (blockId > maxBlockId) {
			maxBlockId = blockId;
		}
		counter.countFor(NumberConversion.long2bytes(blockId));
		// Choose relevant row counter
		long relevantRowCounter = getRelevantRowCounter(globalRowCounter);
		if (((relevantRowCounter % intervalLength) == 0) && (relevantRowCounter > 0)) {
			writeInterval(relevantRowCounter);
			newCounter();
		}
		rowCounter++;
	}

	private void writeInterval(long rowNumber) {
		if ((maxBlockId + 1) > Integer.MAX_VALUE) {
			throw new RuntimeException("PerFileBlockListener does not work for more than Integer.MAX blocks.");
		}
		Long[] intervalValues = new Long[(int) maxBlockId + 1];
		for (byte[] key : counter) {
			int blockId = (int) NumberConversion.bytes2long(key);
			long value = counter.getFrequency(key);
			intervalValues[blockId] = value;
		}
		try {
			csvWriter.print(rowNumber);
			// Don't use printRecord() because the value separator would be missing
			for (Long value : intervalValues) {
				csvWriter.print(value);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void newCounter() {
		if (counter == null) {
			File storeDir = new File(outputPath,
					"blockAccessDB_fileId" + fileId + (alignToGlobal ? "_globalAligned" : ""));
			try {
				FileUtils.deleteDirectory(storeDir);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			RocksDBStore store = new RocksDBStore(storeDir.getAbsolutePath(), maxOpenFiles);
			counter = new PersistantCounter(store);
		} else {
			counter.reset();
		}
	}

	private long getRelevantRowCounter(long globalRowCounter) {
		return alignToGlobal ? globalRowCounter : rowCounter;
	}

	public void close(long globalRowCounter) {
		long relevantRowCounter = getRelevantRowCounter(globalRowCounter);
		writeInterval(relevantRowCounter - 1);
		counter.delete();
		try {
			csvWriter.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
