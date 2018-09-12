package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener;

import java.util.HashMap;
import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogEvent;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogReadListener;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;

public class FileListenerManager implements StorageLogReadListener {

	private final Map<Byte, PerFileCacheListener> fileListener;

	private final boolean alignToGlobal;

	private final String outputPath;

	private long globalRowCounter;

	public FileListenerManager(boolean alignToGlobal, String outputPath) {
		this.alignToGlobal = alignToGlobal;
		this.outputPath = outputPath;
		fileListener = new HashMap<>();

		globalRowCounter = 0;
	}

	@Override
	public void onLogRowRead(int rowType, Map<String, Object> data) {
		if (rowType == StorageLogEvent.READWRITE.ordinal()) {
			byte fileId = (byte) data.get(StorageLogWriter.KEY_FILEID);
			if (!fileListener.containsKey(fileId)) {
				fileListener.put(fileId, new PerFileCacheListener(fileId, alignToGlobal, outputPath));
			}
			fileListener.get(fileId).onLogRowRead(data, globalRowCounter);
			globalRowCounter++;
		}
	}

	@Override
	public void close() {
		fileListener.values().forEach(l -> l.close(globalRowCounter));
	}

}
