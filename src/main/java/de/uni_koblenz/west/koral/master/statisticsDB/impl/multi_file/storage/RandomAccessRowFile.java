package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

public class RandomAccessRowFile implements RowStorage {

	private static final int ESTIMATED_SPACE_PER_LRUCACHE_ENTRY = 128 /* index entry */ + 24 /* Long value */
			+ 64 /* DoublyLinkedNode */;

	final File file;

	RandomAccessFile rowFile;

	final int rowLength;

	/**
	 * Refers to the size of a block that contains exactly how many rows would fit
	 * into a block with the size given in the constructor, but without any
	 * padding/filled space in the end.
	 */
	final int dataLength;

	final int cacheBlockSize;

	/**
	 * Maps RowIds to their rows. Note: It should always be ensured that the values
	 * are not-null (on inserting/updating), because the LRUCache doesn't care and
	 * NullPointerExceptions are not thrown before actually writing to file, when it
	 * is too late to find out where the null came from, in the case of a bug.
	 */
	private final LRUSharedCache<Long, byte[]> fileCache;

	private final ObjectRecycler<byte[]> blockRecycler;

	private final int rowsPerBlock;

	private final boolean rowsAsBlocks;

	private final boolean recycleBlocks;

	private final long fileId;

	private final SharedSpaceManager cacheSpaceManager;

	public RandomAccessRowFile(String storageFilePath, long fileId, int rowLength, SharedSpaceManager cacheSpaceManager,
			int blockSize, boolean recycleBlocks) {
		this.fileId = fileId;
		this.rowLength = rowLength;
		this.cacheSpaceManager = cacheSpaceManager;
		if (blockSize >= rowLength) {
			// Default case: At least one row fits into a block
			rowsAsBlocks = false;
			rowsPerBlock = blockSize / rowLength;
		} else {
			System.err.println("Warning: In RandomAccessRowFile ID " + fileId + " the cache block size (" + blockSize
					+ ") is smaller than row length (" + rowLength + "). Resizing blocks to row length.");
			rowsAsBlocks = true;
			rowsPerBlock = 1;
		}
		dataLength = rowsPerBlock * rowLength;
		// 1 Byte for dirty flag
		cacheBlockSize = dataLength + 1;

		this.recycleBlocks = recycleBlocks;
		if (recycleBlocks) {
			blockRecycler = new ObjectRecycler<>(1024);
		} else {
			blockRecycler = null;
		}

		file = new File(storageFilePath);
		open(true);
		if (cacheSpaceManager != null) {
			// Capacity is calcuated by dividing the available space by estimated space per
			// entry, blockSize is the
			// amount of bytes used for the values in the cache.
//			long maxCacheEntries = maxCacheSize / (ESTIMATED_SPACE_PER_LRUCACHE_ENTRY + cacheBlockSize);
			int entrySize = Long.BYTES + cacheBlockSize;
			fileCache = new LRUSharedCache<Long, byte[]>(cacheSpaceManager, entrySize) {

				@Override
				protected void removeEldest(Long blockId, byte[] block) {
					if (block[dataLength] == 1) {
						// Persist entry before removing
						try {
							writeBlockToFile(blockId, block);
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
						block[dataLength] = 0;
					}
					// Remove from cache
					super.removeEldest(blockId, block);
					if (RandomAccessRowFile.this.recycleBlocks) {
						blockRecycler.dump(block);
					}
				}
			};
		} else {
			fileCache = null;
		}
	}

	@Override
	public void open(boolean createIfNotExists) {
		if (!createIfNotExists && !file.exists()) {
			throw new RuntimeException("FileId " + fileId + ": Could not find file " + file);
		}
		try {
			rowFile = new RandomAccessFile(file, "rw");
		} catch (FileNotFoundException e) {
			throw new RuntimeException("FileId " + fileId + ": Could not find file " + file, e);
		}
	}

	/**
	 * Retrieves a row from <code>file</code>. The offset is calculated by
	 * <code>rowId * row.length</code>.
	 *
	 * @param rowFile
	 *            The RandomAccessFile that will be read
	 * @param rowId
	 *            The row number in the file
	 * @param rowLength
	 *            The length of a row in the specified file
	 * @return The row as a byte array. The returned array has the length of
	 *         rowLength.
	 * @throws IOException
	 */
	@Override
	public byte[] readRow(long rowId) throws IOException {
		if (!valid()) {
			throw new IllegalStateException("FileId " + fileId + ": Cannot operate on a closed storage");
		}
		if (fileCache != null) {
			if (rowsAsBlocks) {
				return readBlock(rowId);
			}
			long blockId = rowId / rowsPerBlock;
			int blockOffset = (int) (rowId % rowsPerBlock) * rowLength;
			byte[] block = readBlock(blockId);
			if (block == null) {
				return null;
			}
			byte[] row = new byte[rowLength];
			System.arraycopy(block, blockOffset, row, 0, rowLength);
			return row;
		} else {
			return readRowFromFile(rowId);
		}

	}

	private byte[] readBlock(long blockId) throws IOException {
		byte[] block = fileCache.get(blockId);
		if (block == null) {
			block = readBlockFromFile(blockId);
			if (block != null) {
				fileCache.put(blockId, block);
			}
		}
		return block;
	}

	private byte[] readBlockFromFile(long blockId) throws IOException {
		if (!valid()) {
			throw new IllegalStateException("FileId " + fileId + ": Cannot operate on a closed storage");
		}
		long offset = blockId * dataLength;
		rowFile.seek(offset);
		byte[] block = null;
		if (recycleBlocks) {
			block = blockRecycler.retrieve();
		}
		if (block == null) {
			block = new byte[cacheBlockSize];
		}
		try {
			rowFile.read(block, 0, dataLength);
		} catch (EOFException e) {
			long fileLength = rowFile.length();
			if ((fileLength > offset) && ((fileLength - offset) < dataLength)) {
				throw new RuntimeException("FileId " + fileId + ": Corrupted database: EOF before block end");
			}
			if (recycleBlocks) {
				blockRecycler.dump(block);
			}
			// Resource does not have an entry (yet)
			return null;
		}
		return block;
	}

	private byte[] readRowFromFile(long rowId) throws IOException {
		if (!valid()) {
			throw new IllegalStateException("FileId " + fileId + ": Cannot operate on a closed storage");
		}
		long offset = rowId * rowLength;
		rowFile.seek(offset);
		byte[] row = new byte[rowLength];
		try {
			rowFile.readFully(row);
		} catch (EOFException e) {
			long fileLength = rowFile.length();
			if ((fileLength > offset) && ((fileLength - offset) < rowLength)) {
				throw new RuntimeException("FileId " + fileId + ": Corrupted database: EOF before row end");
			}
			// Resource does not have an entry (yet)
			return null;
		}
		return row;
	}

	/**
	 * Writes a row into a {@link RandomAccessFile}. The offset is calculated by
	 * <code>rowId * row.length</code>.
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
		if (!valid()) {
			throw new IllegalStateException("FileId " + fileId + ": Cannot operate on a closed storage");
		}
		if (row == null) {
			throw new NullPointerException("FileId " + fileId + ": Row can't be null");
		}
		if (fileCache != null) {
			// We don't handle the rowsAsBlocks case separately because the procedure would
			// be almost identical, because
			// we need to extend the row array for the dirty flag
			long blockId = rowId / rowsPerBlock;
			int blockOffset = (int) (rowId % rowsPerBlock) * rowLength;
			byte[] block = readBlock(blockId);
			if (block == null) {
				if (recycleBlocks) {
					block = blockRecycler.retrieve();
				}
				if (block == null) {
					block = new byte[cacheBlockSize];
				}
			}
			System.arraycopy(row, 0, block, blockOffset, row.length);
			fileCache.update(blockId, block);
			// Set dirty flag (located right behind the data)
			block[dataLength] = 1;
		} else {
			writeRowToFile(rowId, row);
		}
		return true;
	}

	private void writeRowToFile(long rowId, byte[] row) throws IOException {
		rowFile.seek(rowId * rowLength);
		rowFile.write(row);
	}

	private void writeBlockToFile(long blockId, byte[] block) throws IOException {
		rowFile.seek(blockId * dataLength);
		rowFile.write(block, 0, dataLength);
	}

	/**
	 * Returned blocks have a length of {@link #cacheBlockSize}, that is the
	 * dataLength plus one byte for the dirty flag
	 */
	@Override
	public Iterator<Entry<Long, byte[]>> getBlockIterator() throws IOException {
		if (!valid()) {
			throw new IllegalStateException("FileId " + fileId + ": Cannot operate on a closed storage");
		}
		flush();
		long rowFileLength = rowFile.length();
		return new Iterator<Entry<Long, byte[]>>() {

			long blockId = 0;

			@Override
			public boolean hasNext() {
				return ((blockId + 1) * dataLength) <= rowFileLength;
			}

			@Override
			public Entry<Long, byte[]> next() {
				if (!hasNext()) {
					throw new NoSuchElementException("FileId " + fileId + ": Use hasNext() before calling next()");
				}
				try {
					// We ignore the cache to prevent lots of cache updates (one per read) and a not
					// optimal cache
					// content afterwards (the last few blocks would be stored, and not the most
					// frequent used ones).
					byte[] block = readBlockFromFile(blockId);
					if (block == null) {
						// This should never happen because hasNext() checks for this
						throw new IOException("FileId " + fileId + ": Invalid file length");
					}
					if (block.length < dataLength) {
						throw new RuntimeException("FileId " + fileId + ": Block read is too short: " + block.length
								+ " but dataLength is " + dataLength);
					}
					return BlockEntry.getInstance(blockId++, block);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		};
	}

	/**
	 * The cache will be ignored/not filled. Note that the blocks are stored as is.
	 * Obviously they must have the same length.
	 */
	@Override
	public void storeBlocks(Iterator<Entry<Long, byte[]>> blocks) throws IOException {
		if (!valid()) {
			throw new IllegalStateException("FileId " + fileId + ": Cannot operate on a closed storage");
		}
		while (blocks.hasNext()) {
			Entry<Long, byte[]> blockEntry = blocks.next();
			byte[] block = blockEntry.getValue();
			rowFile.seek(blockEntry.getKey() * block.length);
			rowFile.write(block);
		}
	}

	@Override
	public void flush() throws IOException {
		if ((fileCache != null) && (!fileCache.isEmpty())) {
			if (!valid()) {
				throw new IllegalStateException("FileId " + fileId + ": Cannot operate on a closed storage");
			}
			for (Entry<Long, byte[]> entry : fileCache) {
				byte[] block = entry.getValue();
				writeBlockToFile(entry.getKey(), block);
				// Clear dirty flag
				block[dataLength] = 0;
				// TODO: Necessary?
				fileCache.update(entry.getKey(), block);
			}
			if (recycleBlocks && (blockRecycler.retrieved > 0)) {
				System.out.println("FileId " + fileId + " recycled " + blockRecycler.retrieved
						+ " and had a max usage of " + blockRecycler.maxUsage);
				blockRecycler.retrieved = 0;
				blockRecycler.maxUsage = 0;
			}
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
		if (!valid()) {
			throw new IllegalStateException("FileId " + fileId + ": Cannot operate on a closed storage");
		}
		return file.length();
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
		if (fileCache != null) {
			fileCache.clear();
		}
		file.delete();
	}

	@Override
	public void close() {
		try {
			flush();
			rowFile.close();
			if (fileCache != null) {
				fileCache.clear();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean makeRoom() {
		if (fileCache == null) {
			return false;
		}
		return fileCache.makeRoom();
	}

}
