package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import playground.StatisticsDBTest;

/**
 * Singleton class for central managing of subbenchmarks. Calls can be toggled in StatisticsDBTest via SUBBENCHMARKING
 * flag. Events are built as hierarchy, i.e. the time of one task might be included in another as well.
 *
 * There are three "core timings", which are always measured independetly from the subbenchmarking flag. Those are the
 * time for reading the input file and the times for index and extra file operations (including caching etc.)
 * respectively. These core timings are collected via primitive variables to avoid performance overhead, all other
 * measures are collected in an primitive array indexed by the SUBBENCHMARK_EVENT enum.
 *
 * This class is not supposed to be used in production settings, because mainly the subtasks listed in
 * #{@link SUBBENCHMARK_TASK}, which use array accesses, slow down the performance noticeable.
 *
 * @author Philipp Töws
 *
 */
public class SubbenchmarkManager {

	private static SubbenchmarkManager instance;

	private final long[] times;

	private final long[] indexTimes;

	private final long[] extraTimes;

	private long indexTime;

	private long extraTime;

	private long inputTime;

	private long loggingTime;

	public static enum SUBBENCHMARK_TASK {
		MF_INC,
		RLE_RELEASE,
		RLE_RELEASE_ALLOC,
		RLE_RELEASE_ARRAYCOPY,
		RLE_RELEASE_FINDMAXID,
		RLE_RELEASE_FINDBLOCK,
		RLE_NEXT,
		RLE_NEXT_ALLOC,
		RLE_NEXT_ARRAYCOPY,
		ROWMANAGER_CREATE,
		ROWMANAGER_INCREMENT,
		HABSE_FIND,
		HABSE_NOTIFY_ACCESS,
		STORAGEACCESSOR_OPEN,
		SWITCH_TO_FILE,
		IS_ARRAY_ZERO,
		MERGE_DATA_BYTES,
		UPDATE_EXTRA_ROW_ID,
		GET_EXTRA_FILE,
	}

	/**
	 * Subbenchmark tasks that are measured separately dependent on whether it is executed by an index or extra file.
	 *
	 * @author Philipp Töws
	 *
	 */
	public static enum FileSubbenchmarkTask {
		RARF,
		RARF_CACHING,
		RARF_DISKREAD,
		RARF_DISKFLUSH,
		RARF_RAWDISKWRITE,
		RARF_RAWDISKREAD,
		IMRS,
	}

	private SubbenchmarkManager() {
		times = new long[SUBBENCHMARK_TASK.values().length];
		indexTimes = new long[FileSubbenchmarkTask.values().length];
		extraTimes = new long[FileSubbenchmarkTask.values().length];
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
		long start = System.nanoTime();
		times[task.ordinal()] += time;
		loggingTime += System.nanoTime() - start;
	}

	public void addFileTime(long fileId, FileSubbenchmarkTask task, long time) {
		long start = System.nanoTime();
		if (fileId == 0L) {
			indexTimes[task.ordinal()] += time;
		} else {
			extraTimes[task.ordinal()] += time;
		}
		loggingTime += System.nanoTime() - start;
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
		long rest = 0;
		if (StatisticsDBTest.SUBBENCHMARKS) {
			// Calculate time that was recorded in MF_INC event, but doesn't show up in other events
			// Note the task hierarchy, i.e. only the tasks one level below are added
			SUBBENCHMARK_TASK[] mfRecordedTasks = new SUBBENCHMARK_TASK[] {
					SUBBENCHMARK_TASK.RLE_NEXT,
					SUBBENCHMARK_TASK.RLE_RELEASE,
					SUBBENCHMARK_TASK.HABSE_NOTIFY_ACCESS,
					SUBBENCHMARK_TASK.ROWMANAGER_CREATE,
					SUBBENCHMARK_TASK.ROWMANAGER_INCREMENT,
					SUBBENCHMARK_TASK.STORAGEACCESSOR_OPEN,
					SUBBENCHMARK_TASK.SWITCH_TO_FILE,
					SUBBENCHMARK_TASK.IS_ARRAY_ZERO,
					SUBBENCHMARK_TASK.MERGE_DATA_BYTES,
					SUBBENCHMARK_TASK.UPDATE_EXTRA_ROW_ID,
					SUBBENCHMARK_TASK.GET_EXTRA_FILE
			};
			long mfSum = 0;
			for (SUBBENCHMARK_TASK task : mfRecordedTasks) {
				mfSum += times[task.ordinal()];
			}
			mfSum += indexTime + extraTime + loggingTime;
			rest = times[SUBBENCHMARK_TASK.MF_INC.ordinal()] - mfSum;
		}
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
				for (FileSubbenchmarkTask event : FileSubbenchmarkTask.values()) {
					csvPrinter.print("INDEX_" + event.toString());
				}
				for (FileSubbenchmarkTask event : FileSubbenchmarkTask.values()) {
					csvPrinter.print("EXTRA_" + event.toString());
				}
				for (SUBBENCHMARK_TASK event : SUBBENCHMARK_TASK.values()) {
					csvPrinter.print(event.toString());
				}
				csvPrinter.print("LOGGING_TIME");
				csvPrinter.print("REST_MF_INC");
				csvPrinter.println();

			}
			csvPrinter.print(configName);
			csvPrinter.print(totalTimeSec);
			csvPrinter.print(inputTime / 1_000_000_000);
			csvPrinter.print(indexTime / 1_000_000_000);
			csvPrinter.print(extraTime / 1_000_000_000);
			for (int i = 0; i < indexTimes.length; i++) {
				csvPrinter.print(indexTimes[i] / 1_000_000_000);
			}
			for (int i = 0; i < extraTimes.length; i++) {
				csvPrinter.print(extraTimes[i] / 1_000_000_000);
			}
			for (int i = 0; i < times.length; i++) {
				csvPrinter.print(times[i] / 1_000_000_000);
			}
			csvPrinter.print(loggingTime / 1_000_000_000);
			csvPrinter.print(rest / 1_000_000_000);
			csvPrinter.println();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
