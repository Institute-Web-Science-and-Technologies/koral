package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

/**
 * Provides toggleable logging functionality.
 *
 * @author Philipp Töws
 *
 */
public class SimpleLogger {

	private static final boolean LOGGING_ENABLED = false;

	private SimpleLogger() {}

	public static void log(String msg) {
		if (LOGGING_ENABLED) {
			System.out.println(msg);
		}
	}

}
