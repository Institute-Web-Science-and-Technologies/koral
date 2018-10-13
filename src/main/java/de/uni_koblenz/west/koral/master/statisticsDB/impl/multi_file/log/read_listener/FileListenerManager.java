package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener;

import java.util.HashMap;
import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogEvent;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogReadListener;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;

public class FileListenerManager implements StorageLogReadListener {

	private final Map<Byte, PerFileCacheListener> cacheListener;

	private final Map<Byte, PerFileBlockListener> blockListener;

	private final boolean alignToGlobal;

	private final String outputPath;

	private long globalRowCounter;

	private final int maxOpenFilesPerFileId;

	private final long intervalLength;

	public FileListenerManager(boolean alignToGlobal, long intervalLength, int maxOpenFilesPerFileId,
			String outputPath) {
		this.alignToGlobal = alignToGlobal;
		this.intervalLength = intervalLength;
		this.maxOpenFilesPerFileId = maxOpenFilesPerFileId;
		this.outputPath = outputPath;
		cacheListener = new HashMap<>();
		blockListener = new HashMap<>();

		globalRowCounter = 0;
	}

	@Override
	public void onLogRowRead(int rowType, Map<String, Object> data) {
		if (rowType == StorageLogEvent.READWRITE.ordinal()) {
			byte fileId = (byte) data.get(StorageLogWriter.KEY_FILEID);
			if (!cacheListener.containsKey(fileId)) {
				cacheListener.put(fileId, new PerFileCacheListener(fileId, alignToGlobal, outputPath));
				blockListener.put(fileId, new PerFileBlockListener(fileId, intervalLength, maxOpenFilesPerFileId,
						alignToGlobal, outputPath));
			}
			cacheListener.get(fileId).onLogRowRead(data, globalRowCounter);
			blockListener.get(fileId).onLogRowRead(data, globalRowCounter);
			globalRowCounter++;
		} else if (rowType == StorageLogEvent.BLOCKFLUSH.ordinal()) {
			byte fileId = (byte) data.get(StorageLogWriter.KEY_FILEID);
			if (!cacheListener.containsKey(fileId)) {
				// TODO
			}
		}
	}

	@Override
	public void close() {
		cacheListener.values().forEach(l -> l.close(globalRowCounter));
		blockListener.values().forEach(l -> l.close(globalRowCounter));
	}

}
