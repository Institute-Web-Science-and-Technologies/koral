package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class StorageLog {

	public static final String KEY_FILEID = "fileId";
	public static final String KEY_POSITION = "position";

	public static final String KEY_ACCESS_WRITE = "write";
	public static final String KEY_ACCESS_FILESTORAGE = "fileStorage";
	public static final String KEY_ACCESS_CACHEUSAGE = "cacheUsage";
	public static final String KEY_ACCESS_CACHEHIT = "cacheHit";

	public static final String KEY_BLOCKFLUSH_DIRTY = "dirty";

	private static StorageLog storageLog;

	private final CompressedLogWriter logWriter;

	private final Map<String, Object> event;

	private StorageLog(String storagePath) {
		Map<Integer, Map<String, ElementType>> rowLayouts = new HashMap<>();

		Map<String, ElementType> accessEventLayout = new TreeMap<>();
		accessEventLayout.put(KEY_FILEID, ElementType.BYTE);
		accessEventLayout.put(KEY_POSITION, ElementType.INTEGER);
		accessEventLayout.put(KEY_ACCESS_WRITE, ElementType.BIT);
		accessEventLayout.put(KEY_ACCESS_FILESTORAGE, ElementType.BIT);
		accessEventLayout.put(KEY_ACCESS_CACHEUSAGE, ElementType.INTEGER);
		accessEventLayout.put(KEY_ACCESS_CACHEHIT, ElementType.BIT);
		rowLayouts.put(StorageLogEvent.READWRITE.ordinal(), accessEventLayout);

		Map<String, ElementType> blockFlushEventLayout = new TreeMap<>();
		blockFlushEventLayout.put(KEY_FILEID, ElementType.BYTE);
		blockFlushEventLayout.put(KEY_POSITION, ElementType.INTEGER);
		blockFlushEventLayout.put(KEY_BLOCKFLUSH_DIRTY, ElementType.BIT);
		rowLayouts.put(StorageLogEvent.BLOCKFLUSH.ordinal(), blockFlushEventLayout);

		logWriter = new CompressedLogWriter(new File(storagePath, "storageLog.gz"), rowLayouts);

		event = new TreeMap<>();
	}

	public static StorageLog createInstance(String storagePath) {
		storageLog = new StorageLog(storagePath);
		return storageLog;
	}

	public static StorageLog getInstance() {
		return storageLog;
	}

	/**
	 *
	 * @param fileId
	 * @param position
	 *            Which part of the file was accessed. For cached access, this could be the blockId, for file access the
	 *            rowId.
	 * @param write
	 * @param fileStorage
	 * @param cacheUsage
	 * @param cacheHit
	 */
	public void logAcessEvent(long fileId, long position, boolean write, boolean fileStorage, long cacheUsage,
			boolean cacheHit) {
		if ((fileId > Integer.MAX_VALUE) || (position > Integer.MAX_VALUE) || (cacheUsage > Integer.MAX_VALUE)) {
			throw new RuntimeException("Parameters too large for int conversion. Please adjust storage layout");
		}
		event.clear();
		event.put(KEY_FILEID, (int) fileId);
		event.put(KEY_POSITION, (int) position);
		event.put(KEY_ACCESS_WRITE, write);
		event.put(KEY_ACCESS_FILESTORAGE, fileStorage);
		event.put(KEY_ACCESS_CACHEUSAGE, (int) cacheUsage);
		event.put(KEY_ACCESS_CACHEHIT, cacheHit ? 1 : 0);
		logWriter.log(StorageLogEvent.READWRITE.ordinal(), event);
	}

	public void logBlockFlushEvent(long fileId, int blockId, boolean dirty) {
		if ((fileId > Integer.MAX_VALUE) || (blockId > Integer.MAX_VALUE)) {
			throw new RuntimeException("Parameters too large for int conversion. Please adjust storage layout");
		}
		event.clear();
		event.put(KEY_FILEID, (int) fileId);
		event.put(KEY_POSITION, blockId);
		event.put(KEY_BLOCKFLUSH_DIRTY, dirty);
		logWriter.log(StorageLogEvent.BLOCKFLUSH.ordinal(), event);
	}

	public void close() {
		logWriter.close();
	}
}
