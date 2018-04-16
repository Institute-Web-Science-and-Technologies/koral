package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

public class RandomAccessRowFile implements RowStorage {

	final File file;

	RandomAccessFile rowFile;

	final int rowLength;

	/**
	 * Maps RowIds to their rows.
	 */
	private final LRUCache<Long, byte[]> fileCache;

	public RandomAccessRowFile(String storageFilePath, int rowLength, int maxCacheSize) {
		this.rowLength = rowLength;
		file = new File(storageFilePath);
		open(true);

		// TODO: maxCacheSize != capacity of byte arrays
		fileCache = new LRUCache<Long, byte[]>(maxCacheSize) {
			@Override
			protected void removeEldest(Long rowId, byte[] row) {
				// Persist entry before removing
				try {
					writeRowToFile(rowId, row);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				// Remove from cache
				super.removeEldest(rowId, row);
			}
		};
	}

	@Override
	public void open(boolean createIfNotExists) {
		if (!createIfNotExists && !file.exists()) {
			throw new RuntimeException("Could not find file " + file);
		}
		try {
			rowFile = new RandomAccessFile(file, "rw");
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Could not find file " + file, e);
		}
	}

	/**
	 * Retrieves a row from <code>file</code>. The offset is calculated by <code>rowId * row.length</code>.
	 *
	 * @param rowFile
	 *            The RandomAccessFile that will be read
	 * @param rowId
	 *            The row number in the file
	 * @param rowLength
	 *            The length of a row in the specified file
	 * @return The row as a byte array. The returned array has the length of rowLength.
	 * @throws IOException
	 */
	@Override
	public byte[] readRow(long rowId) throws IOException {
		byte[] row = fileCache.get(rowId);
		if (row != null) {
			return row;
		} else {
			row = readRowFromFile(rowId);
			fileCache.put(rowId, row);
			return row;
		}

	}

	public byte[] readRowFromFile(long rowId) throws IOException {
		long offset = rowId * rowLength;
		rowFile.seek(offset);
		byte[] row = new byte[rowLength];
		try {
			rowFile.readFully(row);
		} catch (EOFException e) {
			// Resource does not have an entry (yet)
			return null;
		}
		return row;
	}

	/**
	 * Writes a row into a {@link RandomAccessFile}. The offset is calculated by <code>rowId * row.length</code>.
	 *
	 * @param rowFile
	 *            A RandomAccessFile that will be updated
	 * @param rowId
	 *            The number of the row that will be written
	 * @param row
	 *            The data for the row as byte array
	 * @throws IOException
	 */
	@Override
	public boolean writeRow(long rowId, byte[] row) throws IOException {
		fileCache.update(rowId, row);
		return true;
	}

	public void writeRowToFile(long rowId, byte[] row) throws IOException {
		rowFile.seek(rowId * rowLength);
		rowFile.write(row);
	}

	/**
	 * Throws IOException if there are more than Integer.MAX_VALUE bytes in the file.
	 */
	@Override
	public byte[] getRows() throws IOException {
		// TODO: cache data is missing in the returned array
		throw new UnsupportedOperationException();
//		long fileLength = rowFile.length();
//		if (fileLength > Integer.MAX_VALUE) {
//			throw new IOException("File is too large to fit into a byte array");
//		}
//		byte[] rows = new byte[(int) fileLength];
//		try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
//			in.read(rows);
//		}
//		return rows;
	}

	/**
	 * The cache will be ignored/not filled.
	 */
	@Override
	public void storeRows(byte[] rows) throws IOException {
		try (OutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
			out.write(rows);
		}
	}

	private void flushCache() {

	}

	@Override
	public boolean valid() {
		try {
			return rowFile.getFD().valid();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean isEmpty() {
		return length() == 0;
	}

	@Override
	public long length() {
		try {
			return rowFile.length();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int getRowLength() {
		return rowLength;
	}

	/**
	 * Deletes the underlying file. {@link #close()} must be called beforehand.
	 */
	@Override
	public void delete() {
		file.delete();
	}

	@Override
	public void close() {
		try {
			flushCache();
			rowFile.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
