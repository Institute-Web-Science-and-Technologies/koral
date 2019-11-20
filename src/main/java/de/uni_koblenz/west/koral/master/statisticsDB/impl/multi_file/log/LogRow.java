package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log;

import java.util.Map;

/**
 * Singleton wrapper to read rows of a log containing key-value data.
 * 
 * @author Philipp Töws
 *
 */
public class LogRow {

	private static LogRow instance;

	private int rowType;

	private Map<String, Object> data;

	private LogRow() {}

	static LogRow getInstance(int rowType, Map<String, Object> data) {
		if (instance == null) {
			instance = new LogRow();
		}
		instance.rowType = rowType;
		instance.data = data;
		return instance;
	}

	public int getRowType() {
		return rowType;
	}

	public Map<String, Object> getData() {
		return data;
	}

}
