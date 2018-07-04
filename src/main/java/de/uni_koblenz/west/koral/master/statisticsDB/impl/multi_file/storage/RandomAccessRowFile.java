package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import de.uni_koblenz.west.koral.common.utils.ReusableIDGenerator;

public class RandomAccessRowFile implements RowStorage {

	private static final int ESTIMATED_SPACE_PER_LRUCACHE_ENTRY = 128 /* index entry */ + 24 /* Long value */
			+ 64 /* DoublyLinkedNode */;

	final File file;

	RandomAccessFile rowFile;

	final int rowLength;

	/**
	 * Here, blockSize refers to the size of a block that contains exactly how many rows would fit into a block with the
	 * size given in the constructor, but without any padding/filled space in the end.
	 */
	final int blockSize;

	/**
	 * Maps RowIds to their rows. Note: It should always be ensured that the values are not-null (on
	 * inserting/updating), because the LRUCache doesn't care and NullPointerExceptions are not thrown before actually
	 * writing to file, when it is too late to find out where the null came from, in the case of a bug.
	 */
	private final LRUCache<Long, byte[]> fileCache;

	/**
	 * Stores the dirty bits for each cached block in an RLE list.
	 */
	private final ReusableIDGenerator dirtyBlocks;

	private long writesCausedByFullCache, undirtyWriteAttempts, setTime;

	private final int rowsPerBlock;

	public RandomAccessRowFile(String storageFilePath, int rowLength, long maxCacheSize, int blockSize) {
		this.rowLength = rowLength;
		rowsPerBlock = blockSize / rowLength;
		this.blockSize = rowsPerBlock * rowLength;
		file = new File(storageFilePath);
		open(true);

		if (maxCacheSize > 0) {
			// Capacity is calculated by dividing the available space by estimated space per entry, blockSize is the
			// amount of bytes used for the values in the cache.
			long maxCacheEntries = maxCacheSize / (ESTIMATED_SPACE_PER_LRUCACHE_ENTRY + blockSize);
			fileCache = new LRUCache<Long, byte[]>(maxCacheEntries) {

				@Override
				protected void removeEldest(Long blockId, byte[] block) {
					writesCausedByFullCache++;
					if (dirtyBlocks.isUsed(blockId)) {
						// Persist entry before removing
						try {
							writeBlockToFile(blockId, block);
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
						dirtyBlocks.release(blockId);
					} else {
						undirtyWriteAttempts++;
					}
					// Remove from cache
					super.removeEldest(blockId, block);
				}
			};
			dirtyBlocks = new ReusableIDGenerator();
		} else {
			fileCache = null;
			dirtyBlocks = null;
		}
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
		if (fileCache != null) {
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
		long offset = blockId * blockSize;
		rowFile.seek(offset);
		byte[] block = new byte[blockSize];
		try {
			rowFile.readFully(block);
		} catch (EOFException e) {
			long fileLength = rowFile.length();
			if ((fileLength > offset) && ((fileLength - offset) < blockSize)) {
				throw new RuntimeException("Corrupted database: EOF before block end");
			}
			// Resource does not have an entry (yet)
			return null;
		}
		return block;
	}

	private byte[] readRowFromFile(long rowId) throws IOException {
		long offset = rowId * rowLength;
		rowFile.seek(offset);
		byte[] row = new byte[rowLength];
		try {
			rowFile.readFully(row);
		} catch (EOFException e) {
			long fileLength = rowFile.length();
			if ((fileLength > offset) && ((fileLength - offset) < rowLength)) {
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
		if (!valid()) {
			// TODO: Ensure this everywhere
			throw new RuntimeException("Open Storage before writing to it");
		}
		if (row == null) {
			throw new NullPointerException("Row can't be null");
		}
		if (fileCache != null) {
			long blockId = rowId / rowsPerBlock;
			int blockOffset = (int) (rowId % rowsPerBlock) * rowLength;
//			System.out.println("Writing at " + blockId + " / " + blockOffset);
			byte[] block = readBlock(blockId);
			if (block == null) {
				// TODO: Allocate blocks of block size or only of used data size (i.e. rowsPerBlock * rowLength)?
				block = new byte[blockSize];
			}
			System.arraycopy(row, 0, block, blockOffset, row.length);
			fileCache.update(blockId, block);
			long start = System.nanoTime();
			dirtyBlocks.set(blockId);
			setTime += System.nanoTime() - start;
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
		rowFile.seek(blockId * blockSize);
		rowFile.write(block);
	}

	@Override
	public Iterator<Entry<Long, byte[]>> getBlockIterator() throws IOException {
		flush();
		long rowFileLength = rowFile.length();
		return new Iterator<Entry<Long, byte[]>>() {

			long blockId = 0;
			// In the RAFile, the blocks do not contain fillspace but only data

			@Override
			public boolean hasNext() {
				return ((blockId + 1) * blockSize) <= rowFileLength;
			}

			@Override
			public Entry<Long, byte[]> next() {
				if (!hasNext()) {
					throw new NoSuchElementException("Use hasNext() before calling next()");
				}
				try {
					// TODO: Block arrays created here have the length blockSize/without padding, not the actual given
					// blockSize. These blocks are given to the IMRS which is supposed to work on full blocks
					// TODO: Cache could be used?
					byte[] block = readBlockFromFile(blockId);
					if (block == null) {
						// This should never happen because hasNext() checks for this
						throw new IOException("Invalid file length");
					}
					return BlockEntry.getInstance(blockId++, block);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		};
	}

	/**
	 * The cache will be ignored/not filled.
	 */
	@Override
	public void storeBlocks(Iterator<Entry<Long, byte[]>> blocks) throws IOException {
		while (blocks.hasNext()) {
			Entry<Long, byte[]> blockEntry = blocks.next();
			byte[] block = blockEntry.getValue();
			rowFile.seek(blockEntry.getKey() * blockSize);
			rowFile.write(block, 0, blockSize);
		}
	}

	@Override
	public void flush() throws IOException {
		if (fileCache != null) {
			if (writesCausedByFullCache > 0) {
				System.out.println(file.getName() + ": Total writes caused by full cache: " + writesCausedByFullCache);
				System.out.println(file.getName() + ": Writes saved: " + undirtyWriteAttempts);
			}
			for (Entry<Long, byte[]> entry : fileCache) {
				writeBlockToFile(entry.getKey(), entry.getValue());
			}
			writesCausedByFullCache = 0;
			undirtyWriteAttempts = 0;
			dirtyBlocks.clear();
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
			dirtyBlocks.clear();
		}
		file.delete();
	}

	@Override
	public void close() {
		if (setTime > 0) {
			System.out.println(file.getName() + ": Setting dirty flags took " + (setTime / 1e9) + " sec");
			setTime = 0;
		}
		try {
			flush();
			rowFile.close();
			if (fileCache != null) {
				fileCache.clear();
				dirtyBlocks.clear();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
