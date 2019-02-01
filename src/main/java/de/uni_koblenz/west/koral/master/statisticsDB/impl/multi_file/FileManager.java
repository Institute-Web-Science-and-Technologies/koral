package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import de.uni_koblenz.west.koral.common.io.EncodedLongFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedLongFileOutputStream;
import de.uni_koblenz.west.koral.master.utils.LongIterator;

public class FileManager {

	private static final int DEFAULT_MAX_OPEN_FILES = 1000;

	private final LRUCache<Long, ExtraRowFile> extraFiles;

	private final String storagePath;

	private final File freeSpaceIndexFile;

	private RowFile index;

	public FileManager(String storagePath, int maxOpenFiles) {
		this.storagePath = storagePath;
		if (!this.storagePath.endsWith(File.separator)) {
			storagePath += File.separator;
		}

		extraFiles = new LRUCache<Long, ExtraRowFile>(maxOpenFiles) {
			@Override
			protected void removeEldest(LRUCache<Long, ExtraRowFile>.DoublyLinkedNode eldest) {
				// Don't call super, because this way the ExtraFileRow is retained in the internal index
				eldest.value.close();
			}
		};

		freeSpaceIndexFile = new File(storagePath + "freeSpaceIndex");
		if (freeSpaceIndexFile.exists()) {
			loadExtraFiles();
		}
		setup();
	}

	public FileManager(String storagePath) {
		this(storagePath, DEFAULT_MAX_OPEN_FILES);
	}

	void setup() {
		index = new RowFile(storagePath + "statistics", true);
	}

	/**
	 * Writes a row into the index file.
	 *
	 * @param rowId
	 *            Which row will be (over-)written
	 * @param row
	 *            The bytes of the row, its full length will be written
	 * @throws IOException
	 */
	void writeIndexRow(long rowId, byte[] row) throws IOException {
		index.writeRow(rowId, row);
	}

	/**
	 * Retrieves a row from the index file.
	 *
	 * @param rowId
	 *            Which row to read
	 * @param rowLength
	 *            How long a row in the index file is in bytes
	 * @return The read bytes, with a length of rowLength
	 * @throws IOException
	 */
	byte[] readIndexRow(long rowId, int rowLength) throws IOException {
		return index.readRow(rowId, rowLength);
	}

	/**
	 * Writes a *new* row into an extra file. The optimal offset/row id will be computed and returned afterwards.
	 *
	 * @param fileId
	 *            Which file will be updated
	 * @param row
	 *            The row data, its full length will be written
	 * @return The row id where the row was written into
	 * @throws IOException
	 */
	long writeExternalRow(long fileId, byte[] row) throws IOException {
		ExtraRowFile extraFile = getExtraFile(fileId, true);
		return extraFile.writeRow(row);
	}

	/**
	 * Writes a row into an extra file, with an already specified row id. For inserting a new entry, use
	 * {@link #writeExternalRow(long, byte[])}.
	 *
	 * @param fileId
	 *            Which file to update
	 * @param rowId
	 *            Which row to write
	 * @param row
	 *            The row data, its full length will be written
	 * @throws IOException
	 */
	void writeExternalRow(long fileId, long rowId, byte[] row) throws IOException {
		ExtraRowFile extraFile = getExtraFile(fileId, true);
		extraFile.writeRow(rowId, row);
	}

	/**
	 * Retrieves a row of an extra file.
	 *
	 * @param fileId
	 *            Which extra file to read from
	 * @param rowId
	 *            Which row to read
	 * @param rowLength
	 *            How long the row is in bytes
	 * @return The read bytes with a length of rowLength
	 * @throws IOException
	 */
	byte[] readExternalRow(long fileId, long rowId, int rowLength) throws IOException {
		ExtraRowFile extraFile = getExtraFile(fileId, false);
		return extraFile.readRow(rowId, rowLength);
	}

	/**
	 * Removes a row from an extra file. Internally, it will only be marked as deleted and might be overwritten later
	 * with a new row.
	 *
	 * @param fileId
	 *            In which file the row is located
	 * @param rowId
	 *            Which row to remove
	 */
	void deleteExternalRow(long fileId, long rowId) {
		getExtraFile(fileId, false).deleteRow(rowId);
	}

	/**
	 * Returns a RandomAccessFile instance of the specified extra file. If it needs to be created or re-opened, it's
	 * also registered in the {@link #extraFiles} index.
	 *
	 * @param fileId
	 *            The identifier of the extra file. Equals the metadata bits of a main file row.
	 * @param createIfNotExisting
	 *            If the file should be created if it doesn't exist already
	 * @return A RandomAccessFile instance of the specified extra file
	 * @throws IOException
	 */
	private ExtraRowFile getExtraFile(long fileId, boolean createIfNotExisting) {
		ExtraRowFile extra = extraFiles.get(fileId);
		if (extra == null) {
			extra = new ExtraRowFile(storagePath + String.valueOf(fileId), createIfNotExisting);
			extraFiles.put(fileId, extra);
		}
		if (!extra.valid()) {
			extra.open(createIfNotExisting);
		}
		return extra;
	}

	/**
	 * Deletes all extra files that are empty. Files must be closed with {@link #close()} beforehand.
	 */
	void deleteEmptyFiles() {
		for (ExtraRowFile extraRowFile : extraFiles.values()) {
			if (extraRowFile.isEmpty()) {
				extraRowFile.delete();
			}
		}
	}

	private void loadExtraFiles() {
		try (EncodedLongFileInputStream input = new EncodedLongFileInputStream(freeSpaceIndexFile)) {
			LongIterator iterator = input.iterator();
			long fileId = -1;
			int dataLength = -1;
			long[] list = null;
			int listIndex = 0;
			while (iterator.hasNext()) {
				long l = iterator.next();
				if (fileId == -1) {
					fileId = l;
				} else if (dataLength == -1) {
					dataLength = (int) l;
					list = new long[dataLength];
				} else if (listIndex < dataLength) {
					list[listIndex] = l;
					listIndex++;
					if (listIndex == dataLength) {
						// Reading one entry is done, store and reset everything for next one
						extraFiles.put(fileId, new ExtraRowFile(storagePath + fileId, false, list));
						fileId = -1;
						dataLength = -1;
						list = null;
						listIndex = 0;
					}
				} else {
					iterator.close();
					throw new RuntimeException("Corrupt extra files metadata file");
				}
			}
			iterator.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	void flush() {
		try (EncodedLongFileOutputStream out = new EncodedLongFileOutputStream(freeSpaceIndexFile, false)) {
			Long[] keys = new Long[extraFiles.keySet().size()];
			extraFiles.keySet().toArray(keys);
			Arrays.sort(keys);
			for (long fileId : keys) {
				ExtraRowFile extraFile = extraFiles.get(fileId);
				if (extraFile.isEmpty()) {
					continue;
				}
				long[] data = extraFile.getFreeSpaceIndexData();
				out.writeLong(fileId);
				out.writeLong(data.length);
				for (int i = 0; i < data.length; i++) {
					out.writeLong(data[i]);
				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	/**
	 * @return The length of the index/main file, measured in bytes.
	 */
	long getIndexFileLength() {
		return index.length();
	}

	Map<Long, Long> getFreeSpaceIndexLengths() {
		Map<Long, Long> lengths = new TreeMap<>();
		for (Entry<Long, ExtraRowFile> entry : extraFiles.entrySet()) {
			lengths.put(entry.getKey(), (long) entry.getValue().getFreeSpaceIndexData().length);
		}
		return lengths;
	}

	/**
	 * Clears internal fields, without actually deleting the files.
	 */
	void clear() {
		index = null;
		extraFiles.clear();
	}

	void close() {
		index.close();
		extraFiles.values().forEach(extraRowFile -> extraRowFile.close());
		deleteEmptyFiles();
	}

}
