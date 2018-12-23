package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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

	private final Map<String, Long> times;

	private CentralLogger() {
		times = new HashMap<>();
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
			addTime("FILE_OPERATION_INDEX", time);
		} else {
			addTime("FILE_OPERATION_EXTRA", time);
		}
	}

	public void addTime(String key, long time) {
		Long currentTime = times.get(key);
		if (currentTime == null) {
			currentTime = 0L;
		}
		times.put(key, currentTime + time);
	}

	public void finish() {
		System.out.println("===== CentralLogger:");
		System.out.println("Times:");
		for (Entry<String, Long> e : times.entrySet()) {
			System.out.println(e.getKey() + ": " + StatisticsDBTest.formatTime(e.getValue() / 1_000_000));
		}
		System.out.println("-");
		System.out.println("Total logged time: " + StatisticsDBTest.formatTime(
				(times.values().stream().reduce(Long::sum)).get() / 1_000_000));
		System.out.println("===== End CentralLogger");
	}

	public long getTime(String key) {
		return times.get(key);
	}

}
