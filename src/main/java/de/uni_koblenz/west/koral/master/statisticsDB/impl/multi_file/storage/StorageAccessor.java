package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.logging.Logger;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.SubbenchmarkManager;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.shared_space.HABSESharedSpaceManager;
import playground.StatisticsDBTest;

/**
 * Represents a file of the statistics database. Can use either an {@link InMemoryRowStorage} or a
 * {@link RandomAccessRowFile} as storage backend, and switches it when necessary.
 * 
 * @author Philipp Töws
 *
 */
public class StorageAccessor implements RowStorage {

	protected final long fileId;

	private final Logger logger;

	private final String storageFilePath;

	private final int rowLength;

	private final long maxCacheSize;

	private final HABSESharedSpaceManager cacheSpaceManager;

	private RowStorage cache;

	private RowStorage file;

	RowStorage currentStorage;

	private final int blockSize;

	private final int recyclerCapacity;

	private StorageAccessor(String storageFilePath, long fileId, int rowLength, int blockSize, long maxCacheSize,
			HABSESharedSpaceManager cacheSpaceManager, int recyclerCapacity, boolean createIfNotExisting,
			Logger logger) {
		this.fileId = fileId;
		this.storageFilePath = storageFilePath;
		this.rowLength = rowLength;
		this.blockSize = blockSize;
		this.recyclerCapacity = recyclerCapacity;
		this.logger = logger;
		this.cacheSpaceManager = cacheSpaceManager;

		this.maxCacheSize = maxCacheSize;
		open(createIfNotExisting);
	}

	public StorageAccessor(String storageFilePath, long fileId, int rowLength, int blockSize, long maxCacheSize,
			int recyclerCapacity,
			boolean createIfNotExisting, Logger logger) {
		this(storageFilePath, fileId, rowLength, blockSize, maxCacheSize, null, recyclerCapacity, createIfNotExisting,
				logger);
	}

	public StorageAccessor(String storageFilePath, long fileId, int rowLength, int blockSize,
			HABSESharedSpaceManager cacheSpaceManager,
			int recyclerCapacity, boolean createIfNotExisting, Logger logger) {
		// maxCacheSize is set to zero, because this parameter is only relevant if cacheSpaceManager == null
		this(storageFilePath, fileId, rowLength, blockSize, 0, cacheSpaceManager, recyclerCapacity, createIfNotExisting,
				logger);
	}

	@Override
	public void open(boolean createIfNotExisting) {
		long start = 0;
		if (StatisticsDBTest.SUBBENCHMARKS) {
			start = System.nanoTime();
		}
		if (!new File(storageFilePath).exists() && !createIfNotExisting) {
			throw new RuntimeException("File " + storageFilePath + " does not exist");
		}
		if (cacheSpaceManager == null) {
			// Use fixed parameter as cache size limit
			file = new RandomAccessRowFile(storageFilePath, fileId, rowLength, maxCacheSize, blockSize,
					recyclerCapacity);
			currentStorage = file;
			if (file.length() < maxCacheSize) {
				cache = new InMemoryRowStorage(fileId, rowLength, blockSize, maxCacheSize, this);
			}
		} else {
			// Use the SharedSpaceManager as cache size limit manager
			file = new RandomAccessRowFile(storageFilePath, fileId, rowLength, cacheSpaceManager, this,
					blockSize, recyclerCapacity);
			currentStorage = file;
			long storageLength = file.length();
			if (cacheSpaceManager.isAvailable(storageLength)) {
				cache = new InMemoryRowStorage(fileId, rowLength, blockSize, cacheSpaceManager, this);
				if (!cacheSpaceManager.request(this, storageLength)) {
					// This might also be thrown if this consumer has isAbleToMakeRoomForOwnRequests() set to false
					throw new RuntimeException("Could not request allegedly available space");
				}
			}
		}
		// If the cache was created, fill it with the file contents
		if (cache != null) {
			if (file.length() > 0) {
				if (logger != null) {
					logger.finest("Loading existing storage with path " + storageFilePath);
				}
				try {
					cache.storeBlocks(file.getBlockIterator());
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			} else {
//				System.out.println("File " + fileId + ": Using IMRS");
			}
			currentStorage = cache;
			file.close();
		} else {
//			System.out.println("File " + fileId + ": Using RARF");
		}
		if (StatisticsDBTest.SUBBENCHMARKS) {
			SubbenchmarkManager.getInstance().addTime(SubbenchmarkManager.SUBBENCHMARK_TASK.STORAGEACCESSOR_OPEN,
					System.nanoTime() - start);
		}
	}

	@Override
	public boolean writeRow(long rowId, byte[] row) throws IOException {
		if (cache != null) {
			// Check if row still fits into cache
			if (!cache.writeRow(rowId, row)) {
				switchToFile();
				file.writeRow(rowId, row);
			}
		} else {
			file.writeRow(rowId, row);
		}
		if (cacheSpaceManager != null) {
			cacheSpaceManager.notifyAccess(this);
		}
		return true;
	}

	@Override
	public byte[] readRow(long rowId) throws IOException {
		byte[] result = currentStorage.readRow(rowId);
		if (cacheSpaceManager != null) {
			cacheSpaceManager.notifyAccess(this);
		}
		return result;
	}

	private void switchToFile() {
		long start = 0;
		if (StatisticsDBTest.SUBBENCHMARKS) {
			start = System.nanoTime();
		}
		System.out.println("Switching storage " + fileId + " to file");
		if (logger != null) {
			logger.finest("Switching storage " + fileId + " to file");
		}
		try {
			flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		cache.close();
		cache = null;
		currentStorage = file;
		if (StatisticsDBTest.SUBBENCHMARKS) {
			SubbenchmarkManager.getInstance().addTime(SubbenchmarkManager.SUBBENCHMARK_TASK.SWITCH_TO_FILE,
					System.nanoTime() - start);
		}
	}

	@Override
	public void flush() throws IOException {
		if (!file.valid()) {
			file.open(false);
		}
		if (cache == null) {
			file.flush();
		} else {
			try {
				file.storeBlocks(cache.getBlockIterator());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public Iterator<Entry<Long, byte[]>> getBlockIterator() throws IOException {
		return currentStorage.getBlockIterator();
	}

	@Override
	public void storeBlocks(Iterator<Entry<Long, byte[]>> blocks) throws IOException {
		currentStorage.storeBlocks(blocks);
	}

	@Override
	public boolean valid() {
		return (currentStorage != null) && currentStorage.valid();
	}

	@Override
	public boolean isEmpty() {
		return currentStorage.isEmpty();
	}

	@Override
	public long length() {
		return currentStorage.length();
	}

	@Override
	public int getRowLength() {
		return rowLength;
	}

	/**
	 * For the general StorageAccessor, the length() return value is returned.
	 */
	@Override
	public long accessCosts() {
		// This is not supposed to be used anyways
		return length();
	}

	@Override
	public boolean isAbleToMakeRoomForOwnRequests() {
		if (!valid()) {
			throw new IllegalStateException("Cannot operate on close storage");
		}
		return currentStorage.isAbleToMakeRoomForOwnRequests();
	}

	public long[] getStorageStatistics() {
		return ((RandomAccessRowFile) file).getStorageStatistics();
	}

	@Override
	public void delete() {
		if (cache != null) {
			cache.delete();
		}
		file.delete();
	}

	@Override
	public void close() {
		try {
			flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			if (cache != null) {
				cache.close();
				cache = null;
			}
			// Set to file to allow meaningful length() calls
			currentStorage = file;
			file.close();
		}
	}

	@Override
	public boolean makeRoom() {
		if (cache != null) {
			System.out.println(fileId + " making room by switching to file");
			switchToFile();
			return true;
		} else {
			return file.makeRoom();
		}
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "(ID=" + fileId + ")";
	}
}
