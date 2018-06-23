package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

class InMemoryRowStorage implements RowStorage {

	private static final int DEFAULT_CACHE_BLOCKSIZE = 4096;

	private final int rowLength;

	private Map<Long, byte[]> blocks;

	private final int cacheBlockSize;

	private final long maxBlockCount;

	private final int rowsPerBlock;

	public InMemoryRowStorage(int rowLength, long maxCacheSize, int cacheBlockSize) {
		this.rowLength = rowLength;
		this.cacheBlockSize = cacheBlockSize;
		maxBlockCount = maxCacheSize / cacheBlockSize;
		rowsPerBlock = cacheBlockSize / rowLength;
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
		blocks = new TreeMap<>();
	}

	@Override
	public byte[] readRow(long rowId) throws IOException {
		long blockId = rowId / rowsPerBlock;
		int blockOffset = (int) (rowId % rowsPerBlock) * rowLength;
		byte[] block = blocks.get(blockId);
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
		long blockId = rowId / rowsPerBlock;
		int blockOffset = (int) (rowId % rowsPerBlock) * rowLength;
//		System.out.println("Writing at " + blockId + " / " + blockOffset);
		byte[] block = blocks.get(blockId);
		if (block != null) {
			System.arraycopy(row, 0, block, blockOffset, row.length);
			// TODO: Necessary?
			blocks.put(blockId, block);
			return true;
		} else {
			if (blocks.size() >= maxBlockCount) {
				return false;
			} else {
				// TODO: Allocate blocks of block size or only of used data size (i.e. rowsPerBlock * rowLength)?
				block = new byte[cacheBlockSize];
				System.arraycopy(row, 0, block, blockOffset, row.length);
				blocks.put(blockId, block);
				return true;
			}
		}
	}

	@Override
	public Iterator<Entry<Long, byte[]>> getBlockIterator() {
		return blocks.entrySet().iterator();
	}

	@Override
	public void storeBlocks(Iterator<Entry<Long, byte[]>> rowIterator) throws IOException {
		while (rowIterator.hasNext()) {
			Entry<Long, byte[]> rowEntry = rowIterator.next();
			blocks.put(rowEntry.getKey(), rowEntry.getValue());
		}
	}

	@Override
	public boolean valid() {
		return blocks != null;
	}

	@Override
	public boolean isEmpty() {
		return blocks.isEmpty();
	}

	@Override
	public long length() {
		// TODO: This returns a higher number than the amount of actual rows
		return blocks.size() * rowsPerBlock * rowLength;
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
		blocks = null;
	}

	/**
	 * Clears the storage. Following calls to valid() return false.
	 */
	@Override
	public void close() {
		delete();
	}

}
