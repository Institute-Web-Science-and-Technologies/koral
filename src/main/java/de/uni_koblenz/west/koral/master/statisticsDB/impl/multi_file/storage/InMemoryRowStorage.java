package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

class InMemoryRowStorage implements RowStorage {

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
		if (cacheBlockSize < rowLength) {
			System.err.println(
					"Warning: cache block size is smaller than row length. Falling back to single-block cache.");
			cacheBlockSize = Integer.MAX_VALUE;
		}
		open(true);
	}

	@Override
	public void open(boolean createIfNotExisting) {
		// The createIfNotExisting flag does not matter here, because there is no persisted data that has to be checked
		// for being missing/corrupted.
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
