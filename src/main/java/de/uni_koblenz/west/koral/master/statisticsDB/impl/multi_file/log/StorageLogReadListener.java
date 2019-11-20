package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log;

import java.util.Map;

/**
 * Interface for a listener that receives a row of the storage log as key-value pairs. Usually processes the contained
 * information to generate CSVs for a specific metric.
 *
 * @author Philipp Töws
 *
 */
public interface StorageLogReadListener extends AutoCloseable {

	public void onLogRowRead(int rowType, Map<String, Object> data);

	@Override
	public void close();
}
