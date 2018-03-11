package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.TreeMap;

public class FileManager {

	private RandomAccessFile index;

	private final TreeMap<Long, RandomAccessFile> extraFiles;

	private final FileSpaceIndex fileSpaceIndex;

	private final String storagePath;

	public FileManager(String storagePath) {
		this.storagePath = storagePath;
		if (!this.storagePath.endsWith(File.separator)) {
			storagePath += File.separator;
		}

		extraFiles = new TreeMap<>();
		fileSpaceIndex = new FileSpaceIndex(storagePath);
		setup();
	}

	void setup() {
		try {
			index = new RandomAccessFile(new File(storagePath + "statistics"), "rw");
		} catch (IOException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	void writeIndexRow(long rowId, byte[] row) throws IOException {
		writeRow(index, rowId, row);
	}

	byte[] readIndexRow(long rowId, int rowLength) throws IOException {
		return readRow(index, rowId, rowLength);
	}

	long writeExternalRow(long fileId, byte[] row) throws IOException {
		long rowId = fileSpaceIndex.getFreeRow(fileId);
		RandomAccessFile extraFile = getExtraFile(fileId, true);
		writeRow(extraFile, rowId, row);
		return rowId;
	}

	byte[] readExternalRow(long fileId, long rowId, int rowLength) throws IOException {
		RandomAccessFile extraFile = getExtraFile(fileId, false);
		return readRow(extraFile, rowId, rowLength);
	}

	void deleteExternalRow(long fileId, long rowId) {
		fileSpaceIndex.release(fileId, fileId);
	}

	/**
	 * Retrieves a row from <code>file</code>.
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
	private byte[] readRow(RandomAccessFile file, long rowId, int rowLength) throws IOException {
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
	private void writeRow(RandomAccessFile file, long rowId, byte[] row) throws IOException {
		file.seek(rowId * row.length);
		file.write(row);
	}

	void close() {
		try {
			index.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		for (RandomAccessFile file : extraFiles.values()) {
			try {
				file.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Returns a RandomAccessFile instance of the specified extra file. If it needs to be created or re-opened, it's
	 * also registered in the {@link #extraFiles} index through {@link #openExtraFile(long)}.
	 *
	 * @param fileId
	 *            The identifier of the extra file. Equals the metadata bits of a main file row.
	 * @param createIfNotExisting
	 *            If the file should be created if it doesn't exist already
	 * @return A RandomAccessFile instance of the specified extra file
	 * @throws IOException
	 */
	private RandomAccessFile getExtraFile(long fileId, boolean createIfNotExisting) throws IOException {
		RandomAccessFile extra = extraFiles.get(fileId);
		if ((extra == null) || !extra.getFD().valid()) {
			extra = openExtraFile(fileId, createIfNotExisting);
		}
		return extra;
	}

	/**
	 * Opens a {@link File} and a {@link RandomAccessFile} that is registered in {@link #extraFiles} and returned.
	 *
	 * @param fileId
	 *            The fileId of the extra file that will be opened
	 * @param createIfNotExisting
	 *            If the file should be created if it doesn't exist already
	 * @return A RandomAccessFile instance of the specified extra file
	 * @throws FileNotFoundException
	 */
	private RandomAccessFile openExtraFile(long fileId, boolean createIfNotExisting) throws FileNotFoundException {
		File extraFile = new File(storagePath + fileId);
		if (!extraFile.exists() && !createIfNotExisting) {
			return null;
		}
		RandomAccessFile extra = new RandomAccessFile(extraFile, "rw");
		extraFiles.put(fileId, extra);
		return extra;
	}

	long getIndexFileLength() {
		try {
			return index.length();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	void flush() {
		fileSpaceIndex.flush();
	}

	void clear() {
		index = null;
		extraFiles.clear();
	}

}
