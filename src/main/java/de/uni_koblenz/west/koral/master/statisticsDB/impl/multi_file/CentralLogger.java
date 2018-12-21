package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import playground.StatisticsDBTest;

/**
 * General-purpose singleton class for all kinds of meta-logging. May be used to collect metrics or statistics of
 * different implementation parts without much effort for development/experimenting.
 *
 * @author Philipp Töws
 *
 */
public class CentralLogger {

	private static CentralLogger instance;

	private long habseTime;

	private long totalIndexFileTime;

	private long totalExtraFilesTime;

	private long totalInputReadTime;

	private CentralLogger() {}

	public static CentralLogger getInstance() {
		if (instance == null) {
			instance = new CentralLogger();
		}
		return instance;
	}

	/**
	 * Add time that was needed to find HABSE consumer.
	 *
	 * @param time
	 */
	public void addHABSETime(long time) {
		habseTime += time;
	}

	/**
	 * Add time that was used to execute read/write operations on a file.
	 *
	 * @param fileId
	 * @param time
	 */
	public void addFileOperationTime(long fileId, long time) {
		if (fileId == 0L) {
			totalIndexFileTime += time;
		} else {
			totalExtraFilesTime += time;
		}
	}

	/**
	 * Add time that was used to read the input file/dataset.
	 *
	 * @param time
	 */
	public void addInputReadTime(long time) {
		totalInputReadTime += time;
	}

	public void finish() {
		System.out.println("=== CentralLogger:");
		System.out.println("HABSE total time: " + StatisticsDBTest.formatTime(habseTime / 1_000_000));
		System.out.println("Input read total time: " + StatisticsDBTest.formatTime(totalInputReadTime / 1_000_000));
		System.out.println("Index File Total Time: " + StatisticsDBTest.formatTime(totalIndexFileTime / 1_000_000));
		System.out.println("Extra Files Total Time: " + StatisticsDBTest.formatTime(totalExtraFilesTime / 1_000_000));
		System.out.println("=== End CentralLogger");
	}

	public long getTotalIndexFileTime() {
		return totalIndexFileTime;
	}

	public long getTotalExtraFilesTime() {
		return totalExtraFilesTime;
	}

	public long getTotalInputReadTime() {
		return totalInputReadTime;
	}

}
