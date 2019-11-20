package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener;

import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogReadListener;

/**
 * Listens to log to provide a progression output on stdout.
 *
 * @author Philipp Töws
 *
 */
public class ReadProgressionListener implements StorageLogReadListener {

	private long rowCounter;

	private final long logInterval;

	public ReadProgressionListener(long logInterval) {
		if (logInterval == 0) {
			throw new IllegalArgumentException("Log interval cannot be zero");
		}
		this.logInterval = logInterval;
		rowCounter = 0;
	}

	@Override
	public void onLogRowRead(int rowType, Map<String, Object> data) {
		rowCounter++;
		if ((rowCounter % logInterval) == 0) {
			System.out.println("Read " + String.format("%,d", rowCounter) + " rows");
		}
	}

	public long getCurrentRowCount() {
		return rowCounter;
	}

	@Override
	public void close() {}

}
