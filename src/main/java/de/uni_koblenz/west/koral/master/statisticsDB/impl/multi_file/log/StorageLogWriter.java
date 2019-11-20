package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Is called for different types of accesses and stores the given meta information about these accesses bundled as
 * "events" in a {@link CompressedLogWriter}.
 *
 * @author Philipp Töws
 *
 */
public class StorageLogWriter {

	public static final String KEY_FILEID = "fileId";
	public static final String KEY_POSITION = "position";

	public static final String KEY_CACHEUSAGE = "cacheUsage";
	public static final String KEY_PERCENTAGECACHED = "percentageCached";
	public static final String KEY_ACCESS_WRITE = "write";
	public static final String KEY_ACCESS_FILESTORAGE = "fileStorage";
	public static final String KEY_ACCESS_FILESIZE = "fileSize";
	public static final String KEY_ACCESS_CACHEHIT = "cacheHit";
	public static final String KEY_ACCESS_FOUND = "found";
	public static final String KEY_ACCESS_TIME = "time";

	public static final String KEY_BLOCKFLUSH_DIRTY = "dirty";

	private static StorageLogWriter storageLogWriter;

	private final CompressedLogWriter logWriter;

	private final Map<String, Object> event;

	private boolean finished;

	private StorageLogWriter(String storagePath) {
		Map<Integer, Map<String, ElementType>> rowLayouts = new HashMap<>();

		Map<String, ElementType> accessEventLayout = new TreeMap<>();
		accessEventLayout.put(KEY_FILEID, ElementType.BYTE);
		accessEventLayout.put(KEY_POSITION, ElementType.INTEGER);
		accessEventLayout.put(KEY_ACCESS_WRITE, ElementType.BIT);
		accessEventLayout.put(KEY_ACCESS_FILESTORAGE, ElementType.BIT);
		accessEventLayout.put(KEY_CACHEUSAGE, ElementType.INTEGER);
		accessEventLayout.put(KEY_PERCENTAGECACHED, ElementType.BYTE);
		accessEventLayout.put(KEY_ACCESS_FILESIZE, ElementType.INTEGER);
		accessEventLayout.put(KEY_ACCESS_CACHEHIT, ElementType.BIT);
		accessEventLayout.put(KEY_ACCESS_FOUND, ElementType.BIT);
		accessEventLayout.put(KEY_ACCESS_TIME, ElementType.LONG);
		rowLayouts.put(StorageLogEvent.READWRITE.ordinal(), accessEventLayout);

		Map<String, ElementType> blockFlushEventLayout = new TreeMap<>();
		blockFlushEventLayout.put(KEY_FILEID, ElementType.BYTE);
		blockFlushEventLayout.put(KEY_POSITION, ElementType.INTEGER);
		blockFlushEventLayout.put(KEY_CACHEUSAGE, ElementType.INTEGER);
		blockFlushEventLayout.put(KEY_PERCENTAGECACHED, ElementType.BYTE);
		blockFlushEventLayout.put(KEY_BLOCKFLUSH_DIRTY, ElementType.BIT);
		rowLayouts.put(StorageLogEvent.BLOCKFLUSH.ordinal(), blockFlushEventLayout);

		Map<String, ElementType> chunkSwitchEventLayout = new TreeMap<>();
		rowLayouts.put(StorageLogEvent.CHUNKSWITCH.ordinal(), chunkSwitchEventLayout);

		logWriter = new CompressedLogWriter(new File(storagePath, "storageLog.gz"), rowLayouts);

		for (Entry<Integer, Integer> entry : logWriter.getRowLayoutLengths().entrySet()) {
			int rowLayout = entry.getKey();
			System.out.println("Row Layout " + StorageLogEvent.values()[rowLayout] + " will use " + entry.getValue()
					+ " bytes.");
		}

		event = new TreeMap<>();
		finished = false;
	}

	public static StorageLogWriter createInstance(String storagePath) {
		storageLogWriter = new StorageLogWriter(storagePath);
		return storageLogWriter;
	}

	public static StorageLogWriter getInstance() {
		return storageLogWriter;
	}

	/**
	 *
	 * @param fileId
	 * @param position
	 *            Which part of the file was accessed. For cache-enabled access, this would be the block id, while for
	 *            cache-disabled access it would the rowId. Is used to check how often each chunk is used.
	 * @param write
	 *            Whether this access is a write or read access.
	 * @param fileStorage
	 *            True if a file-based implementation (cached or uncached) is accessed, False for in-memory-only
	 *            implementations.
	 * @param cacheUsage
	 *            Current size of this file only in bytes.
	 * @param cacheHit
	 *            Whether the chunk that contains the row was found in the cache.
	 * @param found
	 *            Whether the requested row was found, either in cache or in file. False means it is a new row.
	 */
	public void logAccessEvent(long fileId, long position, boolean write, boolean fileStorage, long cacheUsage,
			byte percentageCached, long fileSize, boolean cacheHit, boolean found, long time) {
		if (finished) {
			return;
		}
		event.clear();
		event.put(KEY_FILEID, fileId);
		event.put(KEY_POSITION, position);
		event.put(KEY_ACCESS_WRITE, write);
		event.put(KEY_ACCESS_FILESTORAGE, fileStorage);
		event.put(KEY_CACHEUSAGE, cacheUsage);
		event.put(KEY_PERCENTAGECACHED, percentageCached);
		event.put(KEY_ACCESS_FILESIZE, fileSize);
		event.put(KEY_ACCESS_CACHEHIT, cacheHit);
		event.put(KEY_ACCESS_FOUND, found);
		event.put(KEY_ACCESS_TIME, time);
		logWriter.log(StorageLogEvent.READWRITE.ordinal(), event);
	}

	public void logBlockFlushEvent(long fileId, long blockId, long cacheUsage, byte percentageCached, boolean dirty) {
		if (finished) {
			return;
		}
		event.clear();
		event.put(KEY_FILEID, fileId);
		event.put(KEY_POSITION, blockId);
		event.put(KEY_CACHEUSAGE, cacheUsage);
		event.put(KEY_PERCENTAGECACHED, percentageCached);
		event.put(KEY_BLOCKFLUSH_DIRTY, dirty);
		logWriter.log(StorageLogEvent.BLOCKFLUSH.ordinal(), event);
	}

	public void logChunkSwitchEvent() {
		if (finished) {
			return;
		}
		event.clear();
		logWriter.log(StorageLogEvent.CHUNKSWITCH.ordinal(), event);
	}

	public void finish() {
		close();
		finished = true;
	}

	public void close() {
		logWriter.close();
	}
}
