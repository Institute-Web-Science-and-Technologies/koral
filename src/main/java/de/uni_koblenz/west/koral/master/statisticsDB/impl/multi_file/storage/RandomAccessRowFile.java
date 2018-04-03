package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class RandomAccessRowFile implements RowStorage {

	final File file;

	RandomAccessFile rowFile;

	final int rowLength;

	public RandomAccessRowFile(String storageFilePath, int rowLength, boolean createIfNotExists) {
		this.rowLength = rowLength;
		file = new File(storageFilePath);
		open(createIfNotExists);
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
	public void writeRow(long rowId, byte[] row) throws IOException {
		rowFile.seek(rowId * row.length);
		rowFile.write(row);
	}

	@Override
	public boolean valid() {
		try {
			// TODO: Does getFD() throw IOException on closed file?
			return rowFile.getFD().valid();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public long length() {
		try {
			return rowFile.length();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
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
			rowFile.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
