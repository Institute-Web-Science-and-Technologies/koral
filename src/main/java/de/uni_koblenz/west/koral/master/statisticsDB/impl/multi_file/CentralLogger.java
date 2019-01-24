package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import playground.StatisticsDBTest;

/**
 * General-purpose singleton class for all kinds of meta-logging. May be used to collect metrics or statistics of
 * different implementation parts without much effort for development/experimenting. Is not supposed to be used in
 * production settings.
 *
 * @author Philipp Töws
 *
 */
public class CentralLogger {

	private static CentralLogger instance;

	private final long[] times;

	private long indexTime;

	private long extraTime;

	private long inputTime;

	public static enum SUBBENCHMARK_EVENT {
		RLE_NEXT,
		RLE_IS_USED,
		RLE_RELEASE,
		RLE_ALLOC,
		RLE_ARRAYCOPY,
		ROWMANAGER_CREATE,
		ROWMANAGER_INCREMENT,
		MF_INC,
		HABSE_FIND,
		HABSE_NOTIFY_ACCESS,
	}

	private CentralLogger() {
		times = new long[SUBBENCHMARK_EVENT.values().length];
	}

	public static CentralLogger getInstance() {
		if (instance == null) {
			instance = new CentralLogger();
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

	public void addTime(SUBBENCHMARK_EVENT event, long time) {
		times[event.ordinal()] += time;
	}

	public void finish() {
		System.out.println("===== CentralLogger:");
		System.out.println("Times:");
		System.out.println("FILE_INDEX_TIME: " + StatisticsDBTest.formatTime(indexTime / 1_000_000));
		System.out.println("FILE_EXTRA_TIME: " + StatisticsDBTest.formatTime(extraTime / 1_000_000));
		System.out.println("INPUT_READ_TIME: " + StatisticsDBTest.formatTime(inputTime / 1_000_000));
		for (int i = 0; i < times.length; i++) {
			System.out.println(
					SUBBENCHMARK_EVENT.values()[i] + ": " + StatisticsDBTest.formatTime(times[i] / 1_000_000));
		}
		System.out.println("===== End CentralLogger");
	}

	public long getTime(SUBBENCHMARK_EVENT event) {
		return times[event.ordinal()];
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

}
