package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.IOException;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.Utils;

class InMemoryRowStorage implements RowStorage {

	private final int rowLength;

	private final int initialCacheSize;

	private final int maxCacheSize;

	private byte[] rows;

	/**
	 * Index of the last byte of the last row
	 */
	private int lastFilledByteIndex;

	public InMemoryRowStorage(int rowLength, int initialCacheSize, int maxCacheSize) {
		this.rowLength = rowLength;
		this.initialCacheSize = initialCacheSize;
		this.maxCacheSize = maxCacheSize;
		if (initialCacheSize > maxCacheSize) {
			throw new IllegalArgumentException("Initial cache size can't be larger than maximum cache size");
		}
		lastFilledByteIndex = -1;
		open(true);
	}

	@Override
	public void open(boolean createIfNotExisting) {
		rows = new byte[initialCacheSize];
	}

	@Override
	public byte[] readRow(long rowId) throws IOException {
		int offset = (int) (rowId * rowLength);
		if ((offset + rowLength) > rows.length) {
			// Entry does not have an entry (yet)
			return null;
		}
		byte[] data = new byte[rowLength];
		System.arraycopy(rows, offset, data, 0, rowLength);
		return data;
	}

	@Override
	public boolean writeRow(long rowId, byte[] row) throws IOException {
		assert row.length == rowLength;
		int offset = (int) (rowId * rowLength);
		int lastByteIndex = offset + (rowLength - 1);
		if (lastByteIndex < maxCacheSize) {
			// Check if cache has to extend
			if (lastByteIndex > rows.length) {
				int newLength = 2 * rows.length;
				while (newLength < lastByteIndex) {
					// Prevent integer overflow
					if (newLength >= (Integer.MAX_VALUE / 2)) {
						newLength = maxCacheSize;
						break;
					}
					newLength *= 2;
				}
				if (newLength > maxCacheSize) {
					newLength = maxCacheSize;
				}
				rows = Utils.extendArray(rows, newLength - rows.length);
			}
			if (lastByteIndex > lastFilledByteIndex) {
				lastFilledByteIndex = lastByteIndex;
			}
			System.arraycopy(row, 0, rows, offset, rowLength);
			return true;
		} else {
			return false;
		}

	}

	@Override
	public byte[] getRows() {
		byte[] cutRows = new byte[lastFilledByteIndex + 1];
		System.arraycopy(rows, 0, cutRows, 0, lastFilledByteIndex + 1);
		return cutRows;
	}

	/**
	 * The specified array will be used for internal storage, without copying into a new one.
	 */
	@Override
	public void storeRows(byte[] rows) throws IOException {
		this.rows = rows;
		lastFilledByteIndex = rows.length - 1;
	}

	@Override
	public boolean valid() {
		return rows != null;
	}

	@Override
	public boolean isEmpty() {
		return lastFilledByteIndex == -1;
	}

	@Override
	public long length() {
		// lastFilledByteIndex refers to the last written index, therefore we have +1 total bytes written
		return lastFilledByteIndex + 1;
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
		lastFilledByteIndex = -1;
	}

	/**
	 * Clears the storage. Following calls to valid() return false.
	 */
	@Override
	public void close() {
		delete();
	}

}
