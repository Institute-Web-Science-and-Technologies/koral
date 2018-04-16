package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.IOException;

public class StorageAccessor implements RowStorage {

	private static final int DEFAULT_MIN_CACHE_SIZE = 1024;

	private final String storageFilePath;

	private final int rowLength;

	private final int initialCacheSize;

	private final int maxCacheSize;

	private RowStorage cache;

	private RowStorage file;

	private RowStorage currentStorage;

	public StorageAccessor(String storageFilePath, int rowLength, int initialCacheSize, int maxCacheSize) {
		this.storageFilePath = storageFilePath;
		this.rowLength = rowLength;
		this.initialCacheSize = initialCacheSize;

		this.maxCacheSize = maxCacheSize;
		if (initialCacheSize > maxCacheSize) {
			throw new IllegalArgumentException("Initial cache size can't be larger than maximum cache size");
		}
		open(true);
	}

	public StorageAccessor(String storageFilePath, int rowLength, int maxCacheSize) {
		this(storageFilePath, rowLength, Math.min(DEFAULT_MIN_CACHE_SIZE, maxCacheSize), maxCacheSize);
	}

	@Override
	public void open(boolean createIfNotExisting) {
		// TODO: RARF uses the LRUCache object which has overhead for each entry, so there might be more space used than
		// maxCacheSize bytes
		file = new RandomAccessRowFile(storageFilePath, rowLength, maxCacheSize);
		currentStorage = file;
		long storageLength = file.length();
		if (storageLength < maxCacheSize) {
			int cacheSize = initialCacheSize;
			if (storageLength > cacheSize) {
				cacheSize = (int) (2 * storageLength);
			}
			cacheSize = Math.min(cacheSize, maxCacheSize);
			cache = new InMemoryRowStorage(rowLength, cacheSize, maxCacheSize);
			if (storageLength > 0) {
				try {
					cache.storeRows(file.getRows());
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
			currentStorage = cache;
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
		System.out.println("Switching storage to file");
		flushCache();
		cache.close();
		cache = null;
		currentStorage = file;
	}

	private void flushCache() {
		if (cache == null) {
			return;
		}
		try {
			file.storeRows(cache.getRows());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public byte[] getRows() throws IOException {
		return currentStorage.getRows();
	}

	@Override
	public void storeRows(byte[] rows) throws IOException {
		currentStorage.storeRows(rows);
	}

	@Override
	public boolean valid() {
		return currentStorage.valid();
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
			flushCache();
		} finally {
			if (cache != null) {
				cache.close();
			}
			file.close();
		}
	}
}
