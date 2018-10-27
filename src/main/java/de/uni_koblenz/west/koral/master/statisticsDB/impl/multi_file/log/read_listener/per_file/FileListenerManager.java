package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file;

import java.util.HashMap;
import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogEvent;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogReadListener;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;

public class FileListenerManager implements StorageLogReadListener {

	private final Map<Byte, PerFileCacheListener> cacheListener;

	private final Map<Byte, PerFileBlockAccessListener> blockAccessListener;

	private final Map<Byte, PerFileBlockFlushListener> blockFlushListener;

	private final boolean alignToGlobal;

	private final String outputPath;

	private long globalRowCounter;

	private final int maxOpenFilesPerFileId;

	private final long blockAccessIntervalLength;

	private final long blockFlushIntervalLength;

	/**
	 *
	 * @param alignToGlobal
	 * @param intervalLength
	 *            The length of the intervals that are aggregated for block access and flush metrics
	 * @param maxOpenFilesPerFileId
	 * @param outputPath
	 */
	public FileListenerManager(boolean alignToGlobal, long blockAccessIntervalLength, long blockFlushIntervalLength,
			int maxOpenFilesPerFileId,
			String outputPath) {
		this.alignToGlobal = alignToGlobal;
		this.blockAccessIntervalLength = blockAccessIntervalLength;
		this.blockFlushIntervalLength = blockFlushIntervalLength;
		this.maxOpenFilesPerFileId = maxOpenFilesPerFileId;
		this.outputPath = outputPath;
		cacheListener = new HashMap<>();
		blockAccessListener = new HashMap<>();
		blockFlushListener = new HashMap<>();

		globalRowCounter = 0;
	}

	@Override
	public void onLogRowRead(int rowType, Map<String, Object> data) {
		if (rowType == StorageLogEvent.READWRITE.ordinal()) {
			byte fileId = (byte) data.get(StorageLogWriter.KEY_FILEID);
			if (!cacheListener.containsKey(fileId)) {
				cacheListener.put(fileId, new PerFileCacheListener(fileId, alignToGlobal, outputPath));
				blockAccessListener.put(fileId,
						new PerFileBlockAccessListener(fileId, blockAccessIntervalLength, maxOpenFilesPerFileId,
								alignToGlobal, outputPath));
			}
			cacheListener.get(fileId).onLogRowRead(data, globalRowCounter);
			blockAccessListener.get(fileId).onLogRowRead(data, globalRowCounter);
			globalRowCounter++;
		} else if (rowType == StorageLogEvent.BLOCKFLUSH.ordinal()) {
			byte fileId = (byte) data.get(StorageLogWriter.KEY_FILEID);
			if (!blockFlushListener.containsKey(fileId)) {
				blockFlushListener.put(fileId,
						new PerFileBlockFlushListener(fileId, blockFlushIntervalLength, alignToGlobal, outputPath));
			}
			blockFlushListener.get(fileId).onLogRowRead(data, globalRowCounter);
		}
	}

	@Override
	public void close() {
		cacheListener.values().forEach(l -> l.close(globalRowCounter));
		blockAccessListener.values().forEach(l -> l.close(globalRowCounter));
		blockFlushListener.values().forEach(l -> l.close(globalRowCounter));
	}

}
