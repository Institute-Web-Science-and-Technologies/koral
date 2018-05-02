package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;

public class RandomAccessRowFile implements RowStorage {

	private static final int ESTIMATED_SPACE_PER_ENTRY = 128 /* index entry */ + 24 /* Long value */
			+ 64 /* DoublyLinkedNode */;

	final File file;

	RandomAccessFile rowFile;

	final int rowLength;

	/**
	 * Maps RowIds to their rows. Note: It should always be ensured that the values are not-null (on
	 * inserting/updating), because LRUCache doesn't care and the NullPointerException is not thrown before actually
	 * writing to file, when it is too late to find out where the null came from.
	 */
	private final LRUCache<Long, byte[]> fileCache;

	public RandomAccessRowFile(String storageFilePath, int rowLength, int maxCacheSize) {
		this.rowLength = rowLength;
		file = new File(storageFilePath);
		open(true);

		// Capacity is calculated by dividing the available space by estimated space per entry, rowLength refers to the
		// amount of bytes used for the row.
		long capacity = maxCacheSize / (ESTIMATED_SPACE_PER_ENTRY + rowLength);
		fileCache = new LRUCache<Long, byte[]>(capacity) {
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
			if (row == null) {
				return null;
			}
			fileCache.put(rowId, row);
			return row;
		}

	}

	private byte[] readRowFromFile(long rowId) throws IOException {
		long offset = rowId * rowLength;
		rowFile.seek(offset);
		byte[] row = new byte[rowLength];
		try {
			rowFile.readFully(row);
		} catch (EOFException e) {
			long fileLength = rowFile.length();
			if ((fileLength >= offset) && ((fileLength - offset) < rowLength)) {
				throw new RuntimeException("Corrupted database: EOF before row end");
			}
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
		if (row == null) {
			throw new NullPointerException("Row can't be null");
		}
		fileCache.update(rowId, row);
		return true;
	}

	private void writeRowToFile(long rowId, byte[] row) throws IOException {
		rowFile.seek(rowId * rowLength);
		rowFile.write(row);
	}

	/**
	 * Throws IOException if there are resources with ID > Integer.MAX_VALUE in the file or cache.
	 */
	@Override
	public byte[] getRows() throws IOException {
		long fileLength = rowFile.length();
		if (fileLength > Integer.MAX_VALUE) {
			throw new IOException("File is too large to fit into a byte array");
		}
		byte[] rows = new byte[(int) fileLength];
		try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
			in.read(rows);
		}

		// Add/overwrite data from cache
		for (Entry<Long, byte[]> entry : fileCache) {
			long rowId = entry.getKey();
			if (rowId > Integer.MAX_VALUE) {
				throw new IOException("RowFile too large for memory");
			}
			System.arraycopy(entry.getValue(), 0, rows, (int) rowId * rowLength, rowLength);
		}
		return rows;
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

//	@Override
	public void defrag2(long[] freeSpaceIndexData) {
		for (int i = 0; i < freeSpaceIndexData.length; i++) {
			if ((freeSpaceIndexData[i] * rowLength) > Integer.MAX_VALUE) {
				System.err.println("Cannot defrag row file: Fragment(s) too large for memory");
				return;
			}
		}
		try {
			flush();
			long defraggedRows = 0;
			long rowPointer = 0;
			for (int i = 0; i < freeSpaceIndexData.length; i++) {
				if (freeSpaceIndexData[i] == 0) {
					break;
				}
				if (freeSpaceIndexData[i] < 0) {
					rowPointer += -freeSpaceIndexData[i];
					continue;
				}
				if ((i == 0) && (freeSpaceIndexData[i] > 0)) {
					// Skip unfragmented rows at the beginning
					defraggedRows += freeSpaceIndexData[i];
					rowPointer += freeSpaceIndexData[i];
					continue;
				}
				byte[] fragment = new byte[(int) freeSpaceIndexData[i] * rowLength];
				rowFile.seek(rowPointer * rowLength);
				try (InputStream in = Channels.newInputStream(rowFile.getChannel())) {

				}
				rowFile.read(fragment);
				rowFile.seek(defraggedRows * rowLength);
				rowFile.write(fragment);
				defraggedRows += freeSpaceIndexData[i];
				rowPointer += freeSpaceIndexData[i];

			}
			rowFile.setLength(defraggedRows * rowLength);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * After failure, the file is unchanged. The internal cache is cleared.
	 *
	 * @return True on success, false if failed
	 */
	@Override
	public boolean defrag(long[] freeSpaceIndexData) {
		close();
		File tmpOutputFile;
		try {
			tmpOutputFile = new File(file.getCanonicalPath() + ".tmp");
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}
		System.out.println(file);
		System.out.println("FreeSpaceIndexData: " + Arrays.toString(freeSpaceIndexData));
		System.out.println("File size Before defrag: " + String.format("%,d", file.length()));
		// We don't need a BufferedInputStream because IOUtils.copyLarge() buffers internally
		try (InputStream in = new FileInputStream(file)) {
			try (OutputStream out = new BufferedOutputStream(new FileOutputStream(tmpOutputFile))) {
				for (int i = 0; i < freeSpaceIndexData.length; i++) {
					if ((freeSpaceIndexData[i] * rowLength) > Integer.MAX_VALUE) {
						System.err.println("Cannot defrag row file: Fragment(s) too large for memory");
						tmpOutputFile.delete();
						return false;
					}
					if (freeSpaceIndexData[i] == 0) {
						break;
					}
					if (freeSpaceIndexData[i] < 0) {
						IOUtils.skip(in, -freeSpaceIndexData[i] * rowLength);
						continue;
					}
					// Copy the current RLE fragment to the new file
					IOUtils.copyLarge(in, out, 0, freeSpaceIndexData[i] * rowLength);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		file.delete();
		if (!tmpOutputFile.renameTo(file)) {
			throw new RuntimeException("Could not rename file " + tmpOutputFile + " to " + file);
		}
		fileCache.clear();
		System.out.println("File size After defrag: " + String.format("%,d", file.length()));
		return true;
	}

	@Override
	public void flush() throws IOException {
		for (Entry<Long, byte[]> entry : fileCache) {
			writeRowToFile(entry.getKey(), entry.getValue());
		}
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
		fileCache.clear();
		file.delete();
	}

	@Override
	public void close() {
		try {
			flush();
			rowFile.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
