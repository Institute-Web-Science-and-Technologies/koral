package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import de.uni_koblenz.west.koral.common.io.EncodedLongFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedLongFileOutputStream;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.ExtraRowStorage;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.ExtraStorageAccessor;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.LRUCache;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.RowStorage;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.StorageAccessor;
import de.uni_koblenz.west.koral.master.utils.LongIterator;

public class FileManager {

	private static final int DEFAULT_MAX_OPEN_FILES = 1000;

	private static final int DEFAULT_INDEX_FILE_CACHE_SIZE = 100 * 1024 * 1024;

	private static final int INITIAL_INDEX_FILE_CACHE_SIZE = 512 * 1024;

	private static final int DEFAULT_EXTRAFILES_CACHE_SIZE = 10 * 1024 * 1024;

	private final LRUCache<Long, ExtraRowStorage> extraFiles;

	private final String storagePath;

	private final int indexFileCacheSize;

	private final int mainFileRowLength;

	private final File freeSpaceIndexFile;

	private RowStorage index;

	private final int maxExtraCacheSize;

	public FileManager(String storagePath, int mainFileRowLength, int maxExtraFilesAmount, int maxOpenFiles,
			int indexFileCacheSize, int extraFilesCacheSize) {
		this.storagePath = storagePath;
		if (!this.storagePath.endsWith(File.separator)) {
			storagePath += File.separator;
		}
		this.indexFileCacheSize = indexFileCacheSize;
		this.mainFileRowLength = mainFileRowLength;

		// We assume about a quarter of the possible amount of open extra files will be open and distribute each cache
		// size equally.
		maxExtraCacheSize = extraFilesCacheSize / (maxExtraFilesAmount / 4);

		extraFiles = new LRUCache<Long, ExtraRowStorage>(maxOpenFiles) {
			@Override
			protected void removeEldest(Long fileId, ExtraRowStorage storage) {
				// Don't call super, because this way the ExtraFileRow is retained in the internal index
				storage.close();
			}
		};

		freeSpaceIndexFile = new File(storagePath + "freeSpaceIndex");
		setup();
	}

	public FileManager(String storagePath, int mainFileRowLength, int maxExtraFilesAmount) {
		this(storagePath, mainFileRowLength, maxExtraFilesAmount, DEFAULT_MAX_OPEN_FILES, DEFAULT_INDEX_FILE_CACHE_SIZE,
				DEFAULT_EXTRAFILES_CACHE_SIZE);
	}

	void setup() {
		index = new StorageAccessor(storagePath + "statistics", mainFileRowLength, INITIAL_INDEX_FILE_CACHE_SIZE,
				indexFileCacheSize);
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
	byte[] readIndexRow(long rowId) throws IOException {
		return index.readRow(rowId);
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
		ExtraRowStorage extraFile = getOrCreateExtraFile(fileId, row.length);
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
		ExtraRowStorage extraFile = getOrCreateExtraFile(fileId, row.length);
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
	byte[] readExternalRow(long fileId, long rowId) throws IOException {
		ExtraRowStorage extraFile = getExtraFile(fileId);
		return extraFile.readRow(rowId);
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
		getExtraFile(fileId).deleteRow(rowId);
	}

	/**
	 * Returns a ExtraRowStorage instance of the specified extra file. If it needs to be created or re-opened, it's also
	 * registered in the {@link #extraFiles} index.
	 *
	 * @param fileId
	 *            The identifier of the extra file. Equals the metadata bits of a main file row.
	 * @return A RandomAccessFile instance of the specified extra file
	 * @throws IOException
	 */
	private ExtraRowStorage getOrCreateExtraFile(long fileId, int rowLength) {
		ExtraRowStorage extra = extraFiles.get(fileId);
		if (extra == null) {
			extra = new ExtraStorageAccessor(storagePath + String.valueOf(fileId), rowLength, maxExtraCacheSize);
			extraFiles.put(fileId, extra);
		}
		if (!extra.valid()) {
			extra.open(true);
		}
		return extra;
	}

	/**
	 * Returns a ExtraRowStorage instance of the specified extra file, similar to
	 * {@link #getOrCreateExtraFile(long, int)}. This method won't try to create a storage if it doesn't exist, and will
	 * throw an exception instead.
	 *
	 * @param fileId
	 * @return
	 */
	private ExtraRowStorage getExtraFile(long fileId) {
		ExtraRowStorage extra = extraFiles.get(fileId);
		if ((extra != null) && !extra.valid()) {
			extra.open(false);
		}
		return extra;
	}

	/**
	 * Deletes all extra files that are empty. Files must be closed with {@link #close()} beforehand.
	 */
	void deleteEmptyFiles() {
		for (ExtraRowStorage extraRowFile : extraFiles.values()) {
			if (extraRowFile.isEmpty()) {
				extraRowFile.delete();
			}
		}
	}

	void load() {
		try (EncodedLongFileInputStream input = new EncodedLongFileInputStream(freeSpaceIndexFile)) {
			LongIterator iterator = input.iterator();
			long fileId = -1;
			int rowLength = -1;
			int dataLength = -1;
			long[] list = null;
			int listIndex = 0;
			while (iterator.hasNext()) {
				long l = iterator.next();
				if (fileId == -1) {
					fileId = l;
					continue;
				}
				if (rowLength == -1) {
					rowLength = (int) l;
					continue;
				}
				if (dataLength == -1) {
					dataLength = (int) l;
					list = new long[dataLength];
					continue;
				}
				if (listIndex < dataLength) {
					list[listIndex] = l;
				} else {
					// Reading one entry is done, store and reset everything for next one
					extraFiles.put(fileId,
							new ExtraStorageAccessor(storagePath + fileId, rowLength, maxExtraCacheSize, list));
					fileId = -1;
					rowLength = -1;
					dataLength = -1;
					list = null;
					listIndex = 0;
				}

			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	void flush() {
		try (EncodedLongFileOutputStream out = new EncodedLongFileOutputStream(freeSpaceIndexFile, true)) {
			for (Entry<Long, ExtraRowStorage> entry : extraFiles.entrySet()) {
				if (entry.getValue().isEmpty()) {
					continue;
				}
				long[] data = entry.getValue().getFreeSpaceIndexData();
				out.writeLong(entry.getKey());
				out.writeLong(entry.getValue().getRowLength());
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
		for (Entry<Long, ExtraRowStorage> entry : extraFiles.entrySet()) {
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
		extraFiles.values().forEach(extraRowStorage -> extraRowStorage.close());
		deleteEmptyFiles();
	}

}
