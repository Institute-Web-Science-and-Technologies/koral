package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.NotImplementedException;

class InMemoryRowStorage implements RowStorage {

	private static final int DEFAULT_CACHE_BLOCKSIZE = 4096;

	private final int rowLength;

	private final long maxCacheSize;

	private Map<Long, byte[]> rows;

	private final int cacheBlockSize;

	private final long maxBlockId;

	public InMemoryRowStorage(int rowLength, long maxCacheSize, int cacheBlockSize) {
		this.rowLength = rowLength;
		this.maxCacheSize = maxCacheSize;
		this.cacheBlockSize = cacheBlockSize;
		maxBlockId = maxCacheSize / cacheBlockSize;
//		if (maxBlockId > Integer.MAX_VALUE) {
//			throw new IllegalArgumentException("Cache size is too large/Cache block size too small");
//		}
		open(true);
	}

	public InMemoryRowStorage(int rowLength, long maxCacheSize) {
		this(rowLength, maxCacheSize, DEFAULT_CACHE_BLOCKSIZE);
	}

	@Override
	public void open(boolean createIfNotExisting) {
		// TODO: What to do if createIfNotExisting is false?
		rows = new TreeMap<>();
	}

	@Override
	public byte[] readRow(long rowId) throws IOException {
		long offset = rowId * rowLength;
		long blockId = offset / cacheBlockSize;
		int blockOffset = (int) (offset % cacheBlockSize);
		byte[] block = rows.get(blockId);
		if (block != null) {
			byte[] row = new byte[rowLength];
			System.arraycopy(block, blockOffset, row, 0, rowLength);
			return row;
		} else {
			return null;
		}
	}

	@Override
	public boolean writeRow(long rowId, byte[] row) throws IOException {
		long offset = rowId * rowLength;
		long blockId = offset / cacheBlockSize;
		int blockOffset = (int) (offset % cacheBlockSize);
//		System.out.println("Writing at " + blockId + " / " + blockOffset);
		byte[] block = rows.get(blockId);
		if (block != null) {
			System.arraycopy(row, 0, block, blockOffset, row.length);
			// TODO: Necessary?
			rows.put(blockId, block);
			return true;
		} else {
			if (rows.size() >= (maxBlockId - 1)) {
				return false;
			} else {
				block = new byte[cacheBlockSize];
				System.arraycopy(row, 0, block, blockOffset, row.length);
				rows.put(blockId, block);
				return true;
			}
		}
	}

	@Override
	public byte[] getRows() {
		System.err.println("getRows() not implemented yet");
		return new byte[0];
	}

	@Override
	public void storeRows(byte[] rows) throws IOException {
		throw new NotImplementedException("TODO");
	}

	@Override
	public boolean valid() {
		return rows != null;
	}

	@Override
	public boolean isEmpty() {
		return rows.isEmpty();
	}

	@Override
	public long length() {
		return rows.size();
	}

	@Override
	public int getRowLength() {
		return rowLength;
	}

	@Override
	public void flush() {
		// Nothing to flush to
	}

	@Override
	public void delete() {
		rows = null;
	}

	/**
	 * Clears the storage. Following calls to valid() return false.
	 */
	@Override
	public void close() {
		delete();
	}

}
