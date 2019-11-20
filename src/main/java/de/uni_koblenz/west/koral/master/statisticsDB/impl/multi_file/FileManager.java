package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.logging.Logger;

import de.uni_koblenz.west.koral.common.io.EncodedLongFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedLongFileOutputStream;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.FileFlowWatcher;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.ExtraRowStorage;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.ExtraStorageAccessor;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.RowStorage;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.StorageAccessor;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching.Cache;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching.LRUCache;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.shared_space.HABSESharedSpaceManager;
import de.uni_koblenz.west.koral.master.utils.LongIterator;
import playground.StatisticsDBTest;

/**
 * Topmost class for anything storage related. Acts as an adapter to store and reads statistic rows of row
 * storages/files. Also persists and loads metadata of these files for reopening the database.
 *
 * @author Philipp Töws
 *
 */
public class FileManager {

	public static final int DEFAULT_MAX_OPEN_FILES = 1000;

	public static final int DEFAULT_INDEX_FILE_CACHE_SIZE = 100 * 1024 * 1024;

	public static final int DEFAULT_EXTRAFILES_CACHE_SIZE = 10 * 1024 * 1024;

	public static final int DEFAULT_RECYCLER_CAPACITY = 1024;

	public static final int DEFAULT_BLOCK_SIZE = 4096;

	public static final float DEFAULT_HABSE_ACCESSES_WEIGHT = 1.0f;

	public static final int DEFAULT_HABSE_HISTORY_LENGTH = 100_000;

	private final Logger logger;

	private final Cache<Long, ExtraRowStorage> extraFiles;

	private final String storagePath;

	private final long indexFileCacheSize;

	private final int mainFileRowLength;

	private final File extraFilesMetadataFile;

	private RowStorage index;

	private final HABSESharedSpaceManager extraCacheSpaceManager;

	private long maxResourceId;

	private final int blockSize;

	private final int recyclerCapacity;

	public FileManager(String storagePath, int mainFileRowLength, int blockSize, int maxOpenFiles,
			long indexFileCacheSize, long extraFilesCacheSize, int recyclerCapacity, float habseAccessesWeight,
			int habseHistoryLength, Logger logger) {
		this.storagePath = storagePath;
		this.blockSize = blockSize;
		this.indexFileCacheSize = indexFileCacheSize;
		this.mainFileRowLength = mainFileRowLength;
		this.recyclerCapacity = recyclerCapacity;
		this.logger = logger;

		if (extraFilesCacheSize > 0) {
			extraCacheSpaceManager = new HABSESharedSpaceManager(this, extraFilesCacheSize, habseAccessesWeight,
					habseHistoryLength);
		} else {
			extraCacheSpaceManager = null;
		}

		if (StatisticsDBTest.ENABLE_STORAGE_LOGGING) {
			StorageLogWriter.createInstance(storagePath);
		}
		if (StatisticsDBTest.WATCH_FILE_FLOW) {
			FileFlowWatcher.createInstance(storagePath);
		}

		extraFiles = new LRUCache<Long, ExtraRowStorage>(maxOpenFiles) {
			@Override
			protected void removeEldest(Long fileId, ExtraRowStorage storage) {
				// Don't call super so the ExtraFileRow is retained in the internal index
				storage.close();
			}
		};

		extraFilesMetadataFile = new File(storagePath, "extraFilesMetadata");
		if (extraFilesMetadataFile.exists()) {
			loadExtraFiles();
		}
		setup();

	}

	public FileManager(String storagePath, int mainFileRowLength, int maxExtraFilesAmount, Logger logger) {
		this(storagePath, mainFileRowLength, DEFAULT_BLOCK_SIZE, DEFAULT_MAX_OPEN_FILES, DEFAULT_INDEX_FILE_CACHE_SIZE,
				DEFAULT_EXTRAFILES_CACHE_SIZE, DEFAULT_RECYCLER_CAPACITY, DEFAULT_HABSE_ACCESSES_WEIGHT,
				DEFAULT_HABSE_HISTORY_LENGTH, logger);
	}

	void setup() {
		index = new StorageAccessor(storagePath + "statistics", 0, mainFileRowLength, blockSize, indexFileCacheSize, 0,
				true, logger);
	}

	/**
	 * Writes a row into the index file.
	 *
	 * @param resourceId
	 *            Which resource will be (over-)written
	 * @param row
	 *            The bytes of the row, its full length will be written
	 * @throws IOException
	 */
	void writeIndexRow(long resourceId, byte[] row) throws IOException {
		if (!index.valid()) {
			index.open(false);
		}
		// resourceIds start at 1
		index.writeRow(resourceId - 1, row);
		if (resourceId > maxResourceId) {
			maxResourceId = resourceId;
		}
	}

	/**
	 * Retrieves a row from the index file.
	 *
	 * @param resourceId
	 *            Which resource to read
	 * @param rowLength
	 *            How long a row in the index file is in bytes
	 * @return The read bytes, with a length of rowLength
	 * @throws IOException
	 */
	byte[] readIndexRow(long resourceId) throws IOException {
		if (!index.valid()) {
			index.open(false);
		}
		// resourceIds start at 1
		return index.readRow(resourceId - 1);
	}

	/**
	 * Writes a *new* row into an extra file. The optimal offset/row id will be computed and returned afterwards.
	 *
	 * @param fileId
	 *            Which file will be updated
	 * @param row
	 *            The row data, its full length will be written
	 * @return The row id where the row was written into
	 * @throws IOException
	 */
	long writeExternalRow(long fileId, byte[] row) throws IOException {
		ExtraRowStorage extraFile = getOrCreateExtraFile(fileId, row.length);
		return extraFile.writeRow(row);
	}

	/**
	 * Writes a row into an extra file, with an already specified row id. For inserting a new entry, use
	 * {@link #writeExternalRow(long, byte[])}.
	 *
	 * @param fileId
	 *            Which file to update
	 * @param rowId
	 *            Which row to write
	 * @param row
	 *            The row data, its full length will be written
	 * @throws IOException
	 */
	void writeExternalRow(long fileId, long rowId, byte[] row) throws IOException {
		ExtraRowStorage extraFile = getExtraFile(fileId);
		extraFile.writeRow(rowId, row);
	}

	/**
	 * Retrieves a row of an extra file.
	 *
	 * @param fileId
	 *            Which extra file to read from
	 * @param rowId
	 *            Which row to read
	 * @param rowLength
	 *            How long the row is in bytes
	 * @return The read bytes with a length of rowLength
	 * @throws IOException
	 */
	byte[] readExternalRow(long fileId, long rowId) throws IOException {
		ExtraRowStorage extraFile = getExtraFile(fileId);
		return extraFile.readRow(rowId);
	}

	/**
	 * Removes a row from an extra file. Internally, it will only be marked as deleted and might be overwritten later
	 * with a new row.
	 *
	 * @param fileId
	 *            In which file the row is located
	 * @param rowId
	 *            Which row to remove
	 */
	void deleteExternalRow(long fileId, long rowId) {
		getExtraFile(fileId).deleteRow(rowId);
	}

	public Iterator<Entry<Long, ExtraRowStorage>> getLRUExtraFiles() {
		return extraFiles.iteratorFromLast();
	}

	/**
	 * Returns a ExtraRowStorage instance of the specified extra file. If it needs to be created or re-opened, it's also
	 * registered in the {@link #extraFiles} index.
	 *
	 * @param fileId
	 *            The identifier of the extra file. Equals the metadata bits of a main file row.
	 * @return A RandomAccessFile instance of the specified extra file
	 * @throws IOException
	 */
	private ExtraRowStorage getOrCreateExtraFile(long fileId, int rowLength) {
		long start = 0;
		if (StatisticsDBTest.SUBBENCHMARKS) {
			start = System.nanoTime();
		}
		ExtraRowStorage extra = extraFiles.get(fileId);
		if (StatisticsDBTest.SUBBENCHMARKS) {
			SubbenchmarkManager.getInstance().addTime(SubbenchmarkManager.SUBBENCHMARK_TASK.GET_EXTRA_FILE,
					System.nanoTime() - start);
		}
		if (extra == null) {
			extra = new ExtraStorageAccessor(storagePath + String.valueOf(fileId), fileId, rowLength, blockSize,
					extraCacheSpaceManager, recyclerCapacity, null, true, logger);
			extraFiles.put(fileId, extra);
		}
		if (!extra.valid()) {
			extra.open(true);
		}
		return extra;
	}

	/**
	 * Returns a ExtraRowStorage instance of the specified extra file, similar to
	 * {@link #getOrCreateExtraFile(long, int)}. This method won't try to create a storage if it doesn't exist, and will
	 * throw an exception instead.
	 *
	 * @param fileId
	 * @return
	 */
	private ExtraRowStorage getExtraFile(long fileId) {
		long start = 0;
		if (StatisticsDBTest.SUBBENCHMARKS) {
			start = System.nanoTime();
		}
		ExtraRowStorage extra = extraFiles.get(fileId);
		if (StatisticsDBTest.SUBBENCHMARKS) {
			SubbenchmarkManager.getInstance().addTime(SubbenchmarkManager.SUBBENCHMARK_TASK.GET_EXTRA_FILE,
					System.nanoTime() - start);
		}
		if (extra == null) {
			throw new RuntimeException("File " + fileId + " does not exist");
		} else if (!extra.valid()) {
			extra.open(false);
		}
		return extra;
	}

	/**
	 * Deletes all extra files that are empty. Files must be closed with {@link #close()} beforehand.
	 */
	void deleteEmptyFiles() {
		for (Entry<Long, ExtraRowStorage> entry : extraFiles.entrySet()) {
			ExtraRowStorage extraRowFile = entry.getValue();
			if (extraRowFile.isEmpty()) {
				extraRowFile.delete();
				extraFiles.remove(entry.getKey());
			}
		}
	}

	void deleteExtraFile(long fileId) {
		ExtraRowStorage extraRowFile = extraFiles.get(fileId);
		extraRowFile.delete();
		extraFiles.remove(fileId);
	}

	void defragFreeSpaceIndexes() {
		for (ExtraRowStorage extraRowFile : extraFiles.values()) {
			extraRowFile.defragFreeSpaceIndex();
		}
	}

	private void loadExtraFiles() {
		System.out.println("Reading extra files metadata file...");
		try (EncodedLongFileInputStream input = new EncodedLongFileInputStream(extraFilesMetadataFile)) {
			LongIterator iterator = input.iterator();
			if (!iterator.hasNext()) {
				throw new RuntimeException("Metadata File " + extraFilesMetadataFile + " empty");
			}
			maxResourceId = iterator.next();
			System.out.println("Max resource ID: " + maxResourceId);
			long fileId = -1;
			int rowLength = -1;
			int dataLength = -1;
			long[] list = null;
			int listIndex = 0;
			while (iterator.hasNext()) {
				long l = iterator.next();
				if (fileId == -1) {
					fileId = l;
				} else if (rowLength == -1) {
					rowLength = (int) l;
				} else if (dataLength == -1) {
					dataLength = (int) l;
					list = new long[dataLength];
				} else if (listIndex < dataLength) {
					list[listIndex] = l;
					listIndex++;
					if (listIndex == dataLength) {
						// Reading one entry is done, store and reset everything for next one
						extraFiles.put(fileId, new ExtraStorageAccessor(storagePath + fileId, fileId, rowLength,
								blockSize, extraCacheSpaceManager, recyclerCapacity, list, false, logger));
						fileId = -1;
						rowLength = -1;
						dataLength = -1;
						list = null;
						listIndex = 0;
					}
				} else {
					throw new RuntimeException("Corrupt extra files metadata file");
				}
			}
			System.out.println("Loaded extra files metadata.");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	void flush() throws IOException {
		index.flush();
		for (ExtraRowStorage extraStorage : extraFiles.values()) {
			extraStorage.flush();
		}
		try (EncodedLongFileOutputStream out = new EncodedLongFileOutputStream(extraFilesMetadataFile, false)) {
			out.writeLong(maxResourceId);
			Long[] keys = new Long[extraFiles.keySet().size()];
			extraFiles.keySet().toArray(keys);
			Arrays.sort(keys);
			for (long fileId : keys) {
				ExtraRowStorage extraFile = extraFiles.get(fileId);
				if (extraFile.isEmpty()) {
					continue;
				}
				long[] data = extraFile.getFreeSpaceIndexData();
				out.writeLong(fileId);
				out.writeLong(extraFile.getRowLength());
				out.writeLong(data.length);
				for (int i = 0; i < data.length; i++) {
					out.writeLong(data[i]);
				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	/**
	 * @return The length of the index/main file, measured in bytes.
	 */
	long getIndexFileLength() {
		return index.length();
	}

	long getMaxResourceId() {
		return maxResourceId;
	}

	Map<Long, Long> getFreeSpaceIndexLengths() {
		Map<Long, Long> lengths = new TreeMap<>();
		for (Entry<Long, ExtraRowStorage> entry : extraFiles.entrySet()) {
			lengths.put(entry.getKey(), (long) entry.getValue().getFreeSpaceIndexData().length);
		}
		return lengths;
	}

	Map<Long, long[]> getStorageStatistics() {
		Map<Long, long[]> statistics = new HashMap<>();
		statistics.put(0L, ((StorageAccessor) index).getStorageStatistics());
		for (Entry<Long, ExtraRowStorage> entry : extraFiles.entrySet()) {
			StorageAccessor storageAccessor = (StorageAccessor) entry.getValue();
			statistics.put(entry.getKey(), storageAccessor.getStorageStatistics());
		}
		return statistics;
	}

	void close() {
		try {
			flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			index.close();
			extraFiles.values().forEach(extraRowStorage -> extraRowStorage.close());
			deleteEmptyFiles();
			if (StatisticsDBTest.ENABLE_STORAGE_LOGGING) {
				StorageLogWriter.getInstance().close();
			}
			if (StatisticsDBTest.WATCH_FILE_FLOW) {
				FileFlowWatcher.getInstance().close();
			}
		}
	}

}
