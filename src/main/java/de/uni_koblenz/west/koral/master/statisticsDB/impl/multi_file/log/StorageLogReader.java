package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log;

import java.io.File;
import java.util.LinkedList;

public class StorageLogReader {

	private final CompressedLogReader logReader;

	private final LinkedList<StorageLogReadListener> listeners;

	public StorageLogReader(File storageFile) {
		logReader = new CompressedLogReader(storageFile);
		listeners = new LinkedList<>();
	}

	public void registerListener(StorageLogReadListener listener) {
		listeners.add(listener);
	}

	public void read() {
		LogRow logRow;
		while ((logRow = logReader.read()) != null) {
			for (StorageLogReadListener listener : listeners) {
				listener.onLogRowRead(logRow.getRowType(), logRow.getData());
			}
		}
	}

	public void close() {
		logReader.close();
	}

}
