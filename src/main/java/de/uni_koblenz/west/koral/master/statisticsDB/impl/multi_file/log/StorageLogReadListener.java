package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log;

import java.util.Map;

public interface StorageLogReadListener {

	public void onLogRowRead(int rowType, Map<String, Object> data);
}
