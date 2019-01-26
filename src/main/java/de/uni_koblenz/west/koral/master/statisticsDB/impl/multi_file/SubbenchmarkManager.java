package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/**
 * Singleton class for central managing of subbenchmarks. Calls can be toggled in StatisticsDBTest.class via
 * SUBBENCHMARKING flag. Events are built as hierarchy, i.e. the time of one task might be included in another as well.
 *
 * There are three "core timings", which are always measured independetly from the subbenchmarking flag. Those are the
 * time for reading the input file and the times for index and extra file operations (including caching etc.)
 * respectively. These core timings are collected via primitive variables to avoid performance overhead, all other
 * measures are collected in an primitive array indexed by the SUBBENCHMARK_EVENT enum.
 *
 * This class is not supposed to be used in production settings.
 *
 * @author Philipp Töws
 *
 */
public class SubbenchmarkManager {

	private static SubbenchmarkManager instance;

	private final long[] times;

	private long indexTime;

	private long extraTime;

	private long inputTime;

	public static enum SUBBENCHMARK_TASK {
		MF_INC,
		RLE_IS_USED,
		RLE_RELEASE,
		RLE_RELEASE_ALLOC,
		RLE_RELEASE_ARRAYCOPY,
		RLE_RELEASE_FINDMAXID,
		RLE_NEXT,
		RLE_NEXT_ALLOC,
		RLE_NEXT_ARRAYCOPY,
		ROWMANAGER_CREATE,
		ROWMANAGER_INCREMENT,
		HABSE_FIND,
		HABSE_NOTIFY_ACCESS,
	}

	private SubbenchmarkManager() {
		times = new long[SUBBENCHMARK_TASK.values().length];
	}

	public static SubbenchmarkManager getInstance() {
		if (instance == null) {
			instance = new SubbenchmarkManager();
		}
		return instance;
	}

	/**
	 * Add time that was used to execute read/write operations on a file.
	 *
	 * @param fileId
	 * @param time
	 */
	public void addFileOperationTime(long fileId, long time) {
		if (fileId == 0L) {
			indexTime += time;
		} else {
			extraTime += time;
		}
	}

	public void addInputReadTime(long time) {
		inputTime += time;
	}

	public void addTime(SUBBENCHMARK_TASK task, long time) {
		times[task.ordinal()] += time;
	}

	public long getTime(SUBBENCHMARK_TASK task) {
		return times[task.ordinal()];
	}

	public long getInputReadTime() {
		return inputTime;
	}

	public long getIndexTime() {
		return indexTime;
	}

	public long getExtraTime() {
		return extraTime;
	}

	/**
	 *
	 * @param csvFile
	 *            Where the results will be written to as CSV.
	 */
	public void finish(File csvFile, String configName, long totalTimeSec) {
		// Calculate time that was recorded in MF_INC event, but doesn't show up in other events
		// Note the task hierarchy, i.e. only the tasks one level below are added
		long rest = times[SUBBENCHMARK_TASK.MF_INC.ordinal()] -
				(indexTime + extraTime +
						times[SUBBENCHMARK_TASK.RLE_IS_USED.ordinal()] +
						times[SUBBENCHMARK_TASK.RLE_NEXT.ordinal()] +
						times[SUBBENCHMARK_TASK.RLE_RELEASE.ordinal()] +
						times[SUBBENCHMARK_TASK.HABSE_NOTIFY_ACCESS.ordinal()] +
						times[SUBBENCHMARK_TASK.ROWMANAGER_CREATE.ordinal()] +
						times[SUBBENCHMARK_TASK.ROWMANAGER_INCREMENT.ordinal()]);
		CSVFormat csvFileFormat = CSVFormat.RFC4180.withRecordSeparator('\n');
		boolean fileExists = false;
		if (csvFile.exists()) {
			fileExists = true;
		}
		try (CSVPrinter csvPrinter = new CSVPrinter(
				new OutputStreamWriter(new FileOutputStream(csvFile, true), "UTF-8"),
				csvFileFormat)) {
			// Print header for new file
			if (!fileExists) {
				csvPrinter.print("CONFIG");
				csvPrinter.print("TOTAL_TIME");
				csvPrinter.print("INPUT_READ");
				csvPrinter.print("FILE_OPERATIONS_INDEX");
				csvPrinter.print("FILE_OPERATIONS_EXTRA");
				for (SUBBENCHMARK_TASK event : SUBBENCHMARK_TASK.values()) {
					csvPrinter.print(event.toString());
				}
				csvPrinter.print("REST_MF_INC");
				csvPrinter.println();

			}
			csvPrinter.print(configName);
			csvPrinter.print(totalTimeSec);
			csvPrinter.print(inputTime / 1_000_000_000);
			csvPrinter.print(indexTime / 1_000_000_000);
			csvPrinter.print(extraTime / 1_000_000_000);
			for (int i = 0; i < times.length; i++) {
				csvPrinter.print(times[i] / 1_000_000_000);
			}
			csvPrinter.print(rest / 1_000_000_000);
			csvPrinter.println();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
