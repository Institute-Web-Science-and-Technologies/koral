package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.Utils;

public class CachedRowFile extends RandomAccessRowFile {

	private static final int DEFAULT_MIN_CACHE_SIZE = 1024;

	private final int maxCacheSize;

	private byte[] rows;

	private int currentCacheFillSize;

	private LRUCache<Long, byte[]> fileCache;

	public CachedRowFile(String storageFilePath, int rowLength, int initialCacheSize, int maxCacheSize,
			boolean createIfNotExists) {
		super(storageFilePath, rowLength, createIfNotExists);
		this.maxCacheSize = maxCacheSize;
		if (initialCacheSize > maxCacheSize) {
			throw new IllegalArgumentException("Initial cache size can't be larger than maximum cache size");
		}
		if (file.length() > 0) {
			if (file.length() <= maxCacheSize) {
				rows = loadFile();
			} else {
				// File too large for memory, use RandomAccessFile
				rows = null;
			}
		} else {
			rows = new byte[initialCacheSize];
		}
	}

	public CachedRowFile(String storageFilePath, int rowLength, int maxCacheSize, boolean createIfNotExists) {
		this(storageFilePath, rowLength, Math.min(DEFAULT_MIN_CACHE_SIZE, maxCacheSize), maxCacheSize,
				createIfNotExists);
	}

	/**
	 * Loads existing row file into memory. The file length must be less than {@link #maxCacheSize}.
	 *
	 * @return
	 */
	private byte[] loadFile() {
		int fileLength = (int) file.length();
		// Choose smallest possible size for the cache
		int neededBytesForCache = maxCacheSize / 10;
		if (fileLength > neededBytesForCache) {
			neededBytesForCache = maxCacheSize;
		}
		try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
			byte[] data = new byte[neededBytesForCache];
			in.read(data, 0, fileLength);
			currentCacheFillSize = fileLength;
			return data;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void writeRow(long rowId, byte[] row) throws IOException {
		if (rows != null) {
			int offset = (int) (rowId * row.length);
			// Check if row still fits into cache
			if (!writeCache(offset, row)) {
				switchToFile();
				writeRowCached(rowId, row);
			}
		} else {
			writeRowCached(rowId, row);
		}
	}

	private void writeRowCached(long rowId, byte[] row) {
		fileCache.update(rowId, row);
	}

	@Override
	public byte[] readRow(long rowId) throws IOException {
		if (rows != null) {
			int offset = (int) (rowId * rowLength);
			return readCache(offset, rowLength);
		} else {
			return readRowCached(rowId);
		}
	}

	private byte[] readRowCached(long rowId) throws IOException {
		byte[] row = fileCache.get(rowId);
		if (row != null) {
			return row;
		} else {
			row = super.readRow(rowId);
			fileCache.put(rowId, row);
			return row;
		}

	}

	private void switchToFile() {
		fileCache = new LRUCache<Long, byte[]>(maxCacheSize) {
			@Override
			protected void removeEldest(Long rowId, byte[] row) {
				// Persist entry before removing
				try {
					CachedRowFile.super.writeRow(rowId, row);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				super.removeEldest(rowId, row);
			}
		};
		// Copy rows array into fileCache
		for (int i = 0; i < (currentCacheFillSize / rowLength); i++) {
			byte[] row = new byte[rowLength];
			System.arraycopy(rows, i * rowLength, row, 0, rowLength);
			fileCache.put((long) i, row);
		}
		rows = null;
		currentCacheFillSize = 0;
	}

	public boolean writeCache(int offset, byte[] data) {
		int lastByteIndex = offset + data.length;
		if (lastByteIndex < maxCacheSize) {
			// Check if cache has to extend
			if (lastByteIndex > rows.length) {
				int additionalSpace = rows.length;
				if ((rows.length + additionalSpace) > maxCacheSize) {
					additionalSpace = maxCacheSize - rows.length;
				}
				rows = Utils.extendArray(rows, additionalSpace);
			}
			if (lastByteIndex > currentCacheFillSize) {
				currentCacheFillSize = lastByteIndex;
			}
			System.arraycopy(data, 0, rows, offset, data.length);
			return true;
		} else {
			return false;
		}
	}

	public byte[] readCache(int offset, int length) {
		byte[] data = new byte[length];
		System.arraycopy(rows, offset, data, 0, length);
		return data;
	}

	private void flushCache() {
		if (rows == null) {
			return;
		}
		try (OutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
			out.write(rows, 0, currentCacheFillSize);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
		try {
			flushCache();
			rows = null;
		} finally {
			super.close();
		}
	}
}
