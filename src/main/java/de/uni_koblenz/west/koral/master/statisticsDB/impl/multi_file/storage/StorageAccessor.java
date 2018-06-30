package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.logging.Logger;

public class StorageAccessor implements RowStorage {

	private final Logger logger;

	private final String storageFilePath;

	private final int rowLength;

	private final long maxCacheSize;

	private RowStorage cache;

	private RowStorage file;

	RowStorage currentStorage;

	public StorageAccessor(String storageFilePath, int rowLength, long maxCacheSize, Logger logger) {
		this.storageFilePath = storageFilePath;
		this.rowLength = rowLength;
		this.logger = logger;

		this.maxCacheSize = maxCacheSize;
		open(true);
	}

	@Override
	public void open(boolean createIfNotExisting) {
		// TODO: Use the same variable for block sizes of this file and cache below
		file = new RandomAccessRowFile(storageFilePath, rowLength, maxCacheSize, 4096);
		currentStorage = file;
		long storageLength = file.length();
		if (storageLength < maxCacheSize) {
			cache = new InMemoryRowStorage(rowLength, maxCacheSize);
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
		String[] pathElements = storageFilePath.split("/");
		String fileId = pathElements[pathElements.length - 1];
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
				// TODO: Different implementations of file may not flush to disk in storeRows
				// TODO: Data may become incoherent here, if file is defragged and cache not
				// cleared
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
			flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			if (cache != null) {
				if (currentStorage == cache) {
					currentStorage = file;
				}
				cache.close();
				cache = null;
			}
			file.close();
		}
	}
}
