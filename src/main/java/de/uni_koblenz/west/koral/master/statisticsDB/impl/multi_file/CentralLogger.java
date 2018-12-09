package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import playground.StatisticsDBTest;

public class CentralLogger {

	private static CentralLogger instance;

	private long habseTime;

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

	public void finish() {
		System.out.println("=== CentralLogger:");
		System.out.println("HABSE total time: " + StatisticsDBTest.formatTime(habseTime / 1_000_000));
		System.out.println("=== End CentralLogger");
	}

}
