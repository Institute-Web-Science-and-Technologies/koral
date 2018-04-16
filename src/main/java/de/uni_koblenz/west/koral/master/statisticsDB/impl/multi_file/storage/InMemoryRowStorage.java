package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.IOException;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.Utils;

class InMemoryRowStorage implements RowStorage {

	private final int rowLength;

	private final int initialCacheSize;

	private final int maxCacheSize;

	private byte[] rows;

	private int currentCacheFillSize;

	public InMemoryRowStorage(int rowLength, int initialCacheSize, int maxCacheSize) {
		this.rowLength = rowLength;
		this.initialCacheSize = initialCacheSize;
		this.maxCacheSize = maxCacheSize;
		if (initialCacheSize > maxCacheSize) {
			throw new IllegalArgumentException("Initial cache size can't be larger than maximum cache size");
		}
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
//		System.out.println("Rows Length: " + rows.length);
//		System.out.println("offset: " + offset + ", rowLength: " + rowLength);
		System.arraycopy(rows, offset, data, 0, rowLength);
		return data;
	}

	@Override
	public boolean writeRow(long rowId, byte[] row) throws IOException {
		assert row.length == rowLength;
		int offset = (int) (rowId * rowLength);
		int lastByteIndex = offset + rowLength;
		if (lastByteIndex < maxCacheSize) {
			// Check if cache has to extend
			if (lastByteIndex > rows.length) {
				// TODO: int might overflow if lastByteIndex/maxCacheSize is greater than Interger.MAX/2
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
			if (lastByteIndex > currentCacheFillSize) {
				currentCacheFillSize = lastByteIndex;
			}
//			System.out.println("Row: " + Arrays.toString(row));
//			System.out.println("Rows length: " + rows.length);
//			System.out.println("Offset: " + offset + ", rowLength: " + rowLength);
			System.arraycopy(row, 0, rows, offset, rowLength);
			return true;
		} else {
			return false;
		}

	}

	@Override
	public byte[] getRows() {
		byte[] cutRows = new byte[currentCacheFillSize];
		System.arraycopy(rows, 0, cutRows, 0, currentCacheFillSize);
		return cutRows;
	}

	/**
	 * The specified array will be used for internal storage, without copying into a new one.
	 */
	@Override
	public void storeRows(byte[] rows) throws IOException {
		this.rows = rows;
	}

	@Override
	public boolean valid() {
		return rows != null;
	}

	@Override
	public boolean isEmpty() {
		return currentCacheFillSize == 0;
	}

	@Override
	public long length() {
		return currentCacheFillSize;
	}

	@Override
	public int getRowLength() {
		return rowLength;
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
