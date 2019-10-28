package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.SubbenchmarkManager;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.Utils;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.shared_space.SharedSpaceConsumer;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.shared_space.SharedSpaceManager;
import playground.StatisticsDBTest;

/**
 * Row storage that stores the rows bundled as blocks fully in memory, in a HashMap of byte arrays.
 *
 * @author Philipp Töws
 *
 */
class InMemoryRowStorage implements RowStorage {

	private final int rowLength;

	private Map<Long, byte[]> blocks;

	private final int cacheBlockSize;

	private final long maxBlockCount;

	private final SharedSpaceManager cacheSpaceManager;

	private final int rowsPerBlock;

	private final boolean rowsAsBlocks;

	private final long fileId;

	private final SharedSpaceConsumer cacheSpaceConsumer;

	private InMemoryRowStorage(long fileId, int rowLength, int cacheBlockSize, long maxCacheSize,
			SharedSpaceManager cacheSpaceManager, SharedSpaceConsumer cacheSpaceConsumer) {
		this.fileId = fileId;
		this.rowLength = rowLength;
		if (cacheBlockSize >= (2 * rowLength)) {
			// Default case: Multiple rows fit into a block
			rowsAsBlocks = false;
			rowsPerBlock = cacheBlockSize / rowLength;
			// Fit size of block to row data
			this.cacheBlockSize = rowsPerBlock * rowLength;
		} else {
			if (cacheBlockSize < rowLength) {
				System.err
						.println("Warning: In InMemoryRowStorage ID " + fileId + " the cache block size ("
								+ cacheBlockSize
								+ ") is smaller than row length (" + rowLength + "). Resizing blocks to row length.");
			}
			rowsAsBlocks = true;
			rowsPerBlock = 1;
			this.cacheBlockSize = rowLength;
		}
		maxBlockCount = maxCacheSize / this.cacheBlockSize;
		this.cacheSpaceManager = cacheSpaceManager;
		this.cacheSpaceConsumer = cacheSpaceConsumer;

		open(true);
	}

	public InMemoryRowStorage(long fileId, int rowLength, int cacheBlockSize, long maxCacheSize,
			SharedSpaceConsumer cacheSpaceConsumer) {
		this(fileId, rowLength, cacheBlockSize, maxCacheSize, null, cacheSpaceConsumer);
	}

	public InMemoryRowStorage(long fileId, int rowLength, int cacheBlockSize, SharedSpaceManager cacheSpaceManager,
			SharedSpaceConsumer cacheSpaceConsumer) {
		this(fileId, rowLength, cacheBlockSize, -1, cacheSpaceManager, cacheSpaceConsumer);
	}

	@Override
	public void open(boolean createIfNotExisting) {
		// The createIfNotExisting flag does not matter here, because there is no
		// persisted data that has to be checked for being missing/corrupted.
		blocks = new HashMap<>();
	}

	@Override
	public byte[] readRow(long rowId) throws IOException {
		if (!valid()) {
			throw new IllegalStateException("FileId " + fileId + ": Cannot operate on a closed storage");
		}
		if (rowsAsBlocks) {
			return blocks.get(rowId);
		}
		long start = System.nanoTime();
		long blockId = rowId / rowsPerBlock;
		int blockOffset = (int) (rowId % rowsPerBlock) * rowLength;
		byte[] block = blocks.get(blockId);
		byte[] row = null;
		if (block != null) {
			row = new byte[rowLength];
			System.arraycopy(block, blockOffset, row, 0, rowLength);
		}
		onReadRowFinished(start, blockId, row);
		return row;
	}

	/**
	 * Is called when readRow finishes and transmits meta data of this operation for statistical evaluations to the
	 * StorageLog or CentralLogger.
	 */
	private void onReadRowFinished(long startTime, long blockId, byte[] row) {
		long time = System.nanoTime() - startTime;
		if (StatisticsDBTest.ENABLE_STORAGE_LOGGING) {
			boolean found = (row != null) && !Utils.isArrayZero(row);
			StorageLogWriter.getInstance().logAccessEvent(fileId, blockId, false, false, blocks.size() * cacheBlockSize,
					(byte) 100, blocks.size() * cacheBlockSize, true, found, time);
		}
		SubbenchmarkManager.getInstance().addFileOperationTime(fileId, time);
		if (StatisticsDBTest.SUBBENCHMARKS) {
			SubbenchmarkManager.getInstance().addFileTime(fileId, SubbenchmarkManager.FileSubbenchmarkTask.IMRS, time);
		}
	}

	@Override
	public boolean writeRow(long rowId, byte[] row) throws IOException {
		if (!valid()) {
			throw new IllegalStateException("FileId " + fileId + ": Cannot operate on a closed storage");
		}
		if (rowsAsBlocks) {
			if (!spaceForOneBlockAvailable()) {
				return false;
			}
			blocks.put(rowId, row);
			return true;
		}
		long start = System.nanoTime();
		long blockId = rowId / rowsPerBlock;
		int blockOffset = (int) (rowId % rowsPerBlock) * rowLength;
		byte[] block = blocks.get(blockId);
		byte[] readBlock = block;
		boolean result;
		if (block != null) {
			System.arraycopy(row, 0, block, blockOffset, row.length);
			result = true;
		} else {
			if (!spaceForOneBlockAvailable()) {
				result = false;
			} else {
				block = new byte[cacheBlockSize];
				System.arraycopy(row, 0, block, blockOffset, row.length);
				blocks.put(blockId, block);
				result = true;
			}
		}
		onWriteRowFinished(start, blockId, blockOffset, readBlock);
		return result;
	}

	/**
	 * Is called when writeRow finishes and transmits meta data of this operation for statistical evaluations to the
	 * StorageLog or CentralLogger.
	 */
	private void onWriteRowFinished(long startTime, long blockId, int blockOffset, byte[] readBlock) {
		long time = System.nanoTime() - startTime;
		if (StatisticsDBTest.ENABLE_STORAGE_LOGGING) {
			boolean found = false;
			if (readBlock != null) {
				byte[] readRow = new byte[rowLength];
				System.arraycopy(readBlock, blockOffset, readRow, 0, rowLength);
				found = !Utils.isArrayZero(readRow);
			}
			StorageLogWriter.getInstance().logAccessEvent(fileId, blockId, true, false, blocks.size() * cacheBlockSize,
					(byte) 100, blocks.size() * cacheBlockSize, true, found, time);
		}
		SubbenchmarkManager.getInstance().addFileOperationTime(fileId, time);
		if (StatisticsDBTest.SUBBENCHMARKS) {
			SubbenchmarkManager.getInstance().addFileTime(fileId, SubbenchmarkManager.FileSubbenchmarkTask.IMRS, time);
		}
	}

	@Override
	public Iterator<Entry<Long, byte[]>> getBlockIterator() {
		if (!valid()) {
			throw new IllegalStateException("FileId " + fileId + ": Cannot operate on a closed storage");
		}
		return blocks.entrySet().iterator();
	}

	/**
	 * Reserving of shared cache space must be ensured before calling this method.
	 */
	@Override
	public void storeBlocks(Iterator<Entry<Long, byte[]>> blockIterator) throws IOException {
		if (!valid()) {
			throw new IllegalStateException("FileId " + fileId + ": Cannot operate on a closed storage");
		}
		while (blockIterator.hasNext()) {
			Entry<Long, byte[]> blockEntry = blockIterator.next();
			if (blockEntry.getValue().length < cacheBlockSize) {
				throw new RuntimeException("FileId " + fileId + ": Given block too short: "
						+ blockEntry.getValue().length + " but cacheBlockSize is " + cacheBlockSize);
			}
			blocks.put(blockEntry.getKey(), blockEntry.getValue());
		}
	}

	@Override
	public boolean valid() {
		return blocks != null;
	}

	@Override
	public boolean isEmpty() {
		if (!valid()) {
			throw new IllegalStateException("Cannot operate on a closed storage");
		}
		return blocks.isEmpty();
	}

	@Override
	public long length() {
		if (!valid()) {
			throw new IllegalStateException("FileId " + fileId + ": Cannot operate on a closed storage");
		}
		return blocks.size() * rowsPerBlock * rowLength;
	}

	@Override
	public int getRowLength() {
		return rowLength;
	}

	@Override
	public void flush() {
		// Nothing to flush to
	}

	@Override
	public void delete() {
		if (cacheSpaceManager != null) {
			cacheSpaceManager.releaseAll(cacheSpaceConsumer);
		}
		blocks = null;
	}

	/**
	 * Clears the storage. Following calls to valid() return false.
	 */
	@Override
	public void close() {
		delete();
	}

	private boolean spaceForOneBlockAvailable() {
		if (cacheSpaceManager == null) {
			return blocks.size() < maxBlockCount;
		} else {
			return cacheSpaceManager.request(cacheSpaceConsumer, cacheBlockSize);
		}
	}

	@Override
	public boolean makeRoom() {
		// Can't make room in an in-memory-only implementation
		return false;
	}

	@Override
	public boolean isAbleToMakeRoomForOwnRequests() {
		// Can't make room for itself in an in-memory-only implementation
		// This will cause a switch to the file implementation
		return false;
	}

	@Override
	public long accessCosts() {
		// Pretty much equal costs independent of size
		return 1;
	}

}
