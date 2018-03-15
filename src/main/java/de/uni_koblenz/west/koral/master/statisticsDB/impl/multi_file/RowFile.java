package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

class RowFile {

	protected RandomAccessFile file;

	private final String storageFilePath;

	public RowFile(String storageFilePath, boolean createIfNotExists) {
		this.storageFilePath = storageFilePath;
		open(createIfNotExists);
	}

	void open(boolean createIfNotExists) {
		File storageFile = new File(storageFilePath);
		if (!createIfNotExists && !storageFile.exists()) {
			throw new RuntimeException("Could not find file " + storageFilePath);
		}
		try {
			file = new RandomAccessFile(storageFilePath, "rw");
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Could not find file " + storageFilePath, e);
		}
	}

	/**
	 * Retrieves a row from <code>file</code>. The offset is calculated by <code>rowId * row.length</code>.
	 *
	 * @param file
	 *            The RandomAccessFile that will be read
	 * @param rowId
	 *            The row number in the file
	 * @param rowLength
	 *            The length of a row in the specified file
	 * @return The row as a byte array. The returned array has the length of rowLength.
	 * @throws IOException
	 */
	byte[] readRow(long rowId, int rowLength) throws IOException {
		long offset = rowId * rowLength;
		file.seek(offset);
		byte[] row = new byte[rowLength];
		try {
			file.readFully(row);
		} catch (EOFException e) {
			// Resource does not have an entry (yet)
			return null;
		}
		return row;
	}

	/**
	 * Writes a row into a {@link RandomAccessFile}. The offset is calculated by <code>rowId * row.length</code>.
	 *
	 * @param file
	 *            A RandomAccessFile that will be updated
	 * @param rowId
	 *            The number of the row that will be written
	 * @param row
	 *            The data for the row as byte array
	 * @throws IOException
	 */
	void writeRow(long rowId, byte[] row) throws IOException {
		file.seek(rowId * row.length);
		file.write(row);
	}

	boolean valid() {
		try {
			// TODO: Does getFD() throw IOException on closed file?
			return file.getFD().valid();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	long length() {
		try {
			return file.length();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Deletes the underlying file. {@link #close()} must be called beforehand.
	 */
	void delete() {
		new File(storageFilePath).delete();
	}

	void close() {
		try {
			file.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
