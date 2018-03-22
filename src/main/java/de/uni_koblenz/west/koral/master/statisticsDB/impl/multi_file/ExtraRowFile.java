package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import de.uni_koblenz.west.koral.common.utils.ReusableIDGenerator;

class ExtraRowFile extends RowFile implements ByteCache {

	private final ReusableIDGenerator freeSpaceIndex;

	private final int maxCacheSize;

	private byte[] cache;

	private int currentCacheFillSize;

	public ExtraRowFile(String storageFilePath, int maxCacheSize, boolean createIfNotExists) {
		this(storageFilePath, maxCacheSize, createIfNotExists, null);
	}

	public ExtraRowFile(String storageFilePath, int maxCacheSize, boolean createIfNotExists, long[] list) {
		super(storageFilePath, createIfNotExists);
		this.maxCacheSize = maxCacheSize;
		if (file.length() > 0) {
			if (file.length() <= maxCacheSize) {
				cache = loadFile();
			} else {
				// File too large for caching, use RandomAccessFile
				cache = null;
			}
		} else {
			cache = new byte[maxCacheSize / 10];
		}
		freeSpaceIndex = new ReusableIDGenerator(list);
	}

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

	long writeRow(byte[] row) throws IOException {
		long rowId = freeSpaceIndex.getNextId();
		writeRow(rowId, row);
		return rowId;
	}

	@Override
	void writeRow(long rowId, byte[] row) throws IOException {
		if (cache != null) {
			int offset = (int) (rowId * row.length);
			// Check if row still fits into cache
			if (!writeCache(offset, row)) {
				switchToFile();
				super.writeRow(rowId, row);
			}
		} else {
			super.writeRow(rowId, row);
		}
	}

	@Override
	byte[] readRow(long rowId, int rowLength) throws IOException {
		if (cache != null) {
			int offset = (int) (rowId * rowLength);
			return readCache(offset, rowLength);
		} else {
			return super.readRow(rowId, rowLength);
		}
	}

	@Override
	public boolean writeCache(int offset, byte[] data) {
		int lastByteIndex = offset + data.length;
		if (lastByteIndex < maxCacheSize) {
			// Check if cache has to extend to max size
			if (lastByteIndex > cache.length) {
				cache = Utils.extendArray(cache, maxCacheSize - cache.length);
			}
			if (lastByteIndex > currentCacheFillSize) {
				currentCacheFillSize = lastByteIndex;
			}
			System.arraycopy(data, 0, cache, offset, data.length);
			return true;
		} else {
			return false;
		}
	}

	@Override
	public byte[] readCache(int offset, int length) {
		byte[] data = new byte[length];
		System.arraycopy(cache, offset, data, 0, length);
		return data;
	}

	@Override
	public byte[] getData() {
		return cache;
	}

	private void switchToFile() {
		flushCache();
		cache = null;
		currentCacheFillSize = 0;
	}

	private void flushCache() {
		if (cache == null) {
			return;
		}
		try (OutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
			out.write(cache, 0, currentCacheFillSize);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	void deleteRow(long rowId) {
		freeSpaceIndex.release(rowId);
	}

	long[] getFreeSpaceIndexData() {
		return freeSpaceIndex.getData();
	}

	boolean isEmpty() {
		return freeSpaceIndex.isEmpty();
	}

	@Override
	void close() {
		try {
			flushCache();
			cache = null;
		} finally {
			super.close();
		}
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + '@' + Integer.toHexString(hashCode());
	}

}
