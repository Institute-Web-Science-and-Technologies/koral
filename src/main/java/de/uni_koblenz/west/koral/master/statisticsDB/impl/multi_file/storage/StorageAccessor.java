package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.logging.Logger;

public class StorageAccessor implements RowStorage {

	private static final int DEFAULT_CACHE_BLOCKSIZE = 4096;

	private final long fileId;

	private final Logger logger;

	private final String storageFilePath;

	private final int rowLength;

	private final long maxCacheSize;

	private final SharedSpaceManager extraCacheSpaceManager;

	private final boolean recycleBlocks;

	private RowStorage cache;

	private RowStorage file;

	RowStorage currentStorage;

	public StorageAccessor(String storageFilePath, long fileId, int rowLength, long maxCacheSize,
			SharedSpaceManager extraCacheSpaceManager, boolean recycleBlocks, boolean createIfNotExisting,
			Logger logger) {
		this.fileId = fileId;
		this.storageFilePath = storageFilePath;
		this.rowLength = rowLength;
		this.logger = logger;
		this.extraCacheSpaceManager = extraCacheSpaceManager;
		this.recycleBlocks = recycleBlocks;

		this.maxCacheSize = maxCacheSize;
		open(createIfNotExisting);
	}

	@Override
	public void open(boolean createIfNotExisting) {
		if (!new File(storageFilePath).exists() && !createIfNotExisting) {
			throw new RuntimeException("File " + storageFilePath + " does not exist");
		}
		// TODO: Extract cache blocksize as own parameter to CLI/config
		file = new RandomAccessRowFile(storageFilePath, fileId, rowLength, maxCacheSize, DEFAULT_CACHE_BLOCKSIZE,
				recycleBlocks);
		currentStorage = file;
		long storageLength = file.length();
		// TODO: Add own flag for this condition or unify implementations
		if (!recycleBlocks) {
			// Index storage
			if (storageLength < maxCacheSize) {
				cache = new InMemoryRowStorage(fileId, rowLength, DEFAULT_CACHE_BLOCKSIZE, maxCacheSize);
			}
		} else {
			// Extra storage
			if (extraCacheSpaceManager.request(storageLength)) {
				cache = new InMemoryRowStorage(fileId, rowLength, DEFAULT_CACHE_BLOCKSIZE, extraCacheSpaceManager);
			}
		}
		// If the cache was created, fill it with the file contents
		if (cache != null) {
			if (storageLength > 0) {
				if (logger != null) {
					logger.finest("Loading existing storage with path " + storageFilePath);
				}
				try {
					cache.storeBlocks(file.getBlockIterator());
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
			currentStorage = cache;
			file.close();
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
		return true;
	}

	@Override
	public byte[] readRow(long rowId) throws IOException {
		byte[] result = currentStorage.readRow(rowId);
		return result;
	}

	private void switchToFile() {
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
			file.close();
		}
	}
}
