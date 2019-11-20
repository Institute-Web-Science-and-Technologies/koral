package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.SubbenchmarkManager;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.Utils;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching.Cache;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching.LRUCache;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching.ObjectRecycler;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.shared_space.LRUSharedCache;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.shared_space.SharedSpaceConsumer;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.shared_space.SharedSpaceManager;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import playground.StatisticsDBTest;

/**
 * Row storage that stores its rows bundled as blocks in a
 * {@link RandomAccessFile} but additionally provides a caching layer for the
 * blocks. If a {@link SharedSpaceManager} is given, a special
 * {@link LRUSharedCache} is used.
 *
 *
 * @author Philipp Töws
 *
 */
public class RandomAccessRowFile implements RowStorage {

  private static final int ESTIMATED_SPACE_PER_LRUCACHE_ENTRY_WITHOUT_DATA = 128 /*
                                                                                  * index
                                                                                  * entry
                                                                                  */
          + 24 /* Long value */
          + 64 /* DoublyLinkedNode */;

  final File file;

  RandomAccessFile rowFile;

  private long fileLength;

  final int rowLength;

  /**
   * Refers to the size of a block that contains exactly how many rows would fit
   * into a block with the size given in the constructor, but without any
   * padding/filled space in the end.
   */
  final int dataLength;

  final int cacheBlockSize;

  /**
   * Maps RowIds to their rows. Note: It should always be ensured that the
   * values are not-null (on inserting/updating), because the LRUCache doesn't
   * care and NullPointerExceptions are not thrown before actually writing to
   * file, when it is too late to find out where the null came from, in the case
   * of a bug.
   */
  private final Cache<Long, byte[]> fileCache;

  private final ObjectRecycler<byte[]> blockRecycler;

  private final int estimatedSpacePerCacheEntry;

  private final int rowsPerBlock;

  private final boolean rowsAsBlocks;

  private final long fileId;

  private final Set<Long> uncachedBlocks;

  // Metastatistics for read benchmarks
  private long cacheHits, cacheMisses, notExisting;

  private final int recyclerCapacity;

  private RandomAccessRowFile(String storageFilePath, long fileId, int rowLength, long maxCacheSize,
          SharedSpaceManager cacheSpaceManager, SharedSpaceConsumer cacheSpaceConsumer,
          int blockSize, int recyclerCapacity) {
    this.fileId = fileId;
    this.rowLength = rowLength;
    this.recyclerCapacity = recyclerCapacity;
    if (blockSize >= (2 * rowLength)) {
      // Default case: Mulriple rows fit into a block
      rowsAsBlocks = false;
      rowsPerBlock = blockSize / rowLength;
    } else {
      if (blockSize < rowLength) {
        System.err.println("Warning: In RandomAccessRowFile ID " + fileId
                + " the cache block size (" + blockSize + ") is smaller than row length ("
                + rowLength + "). Resizing blocks to row length.");
      }
      rowsAsBlocks = true;
      rowsPerBlock = 1;
    }
    dataLength = rowsPerBlock * rowLength;
    // 1 Byte for dirty flag
    cacheBlockSize = dataLength + 1;
    estimatedSpacePerCacheEntry = RandomAccessRowFile.ESTIMATED_SPACE_PER_LRUCACHE_ENTRY_WITHOUT_DATA
            + cacheBlockSize;

    if (recyclerCapacity > 0) {
      blockRecycler = new ObjectRecycler<>(recyclerCapacity);
    } else {
      blockRecycler = null;
    }
    uncachedBlocks = new HashSet<>();

    file = new File(storageFilePath);
    fileLength = file.length();
    open(true);
    if (cacheSpaceManager != null) {
      fileCache = new LRUSharedCache<Long, byte[]>(cacheSpaceManager, cacheSpaceConsumer,
              estimatedSpacePerCacheEntry) {
        @Override
        protected void removeEldest(Long blockId, byte[] block) {
          onRemoveEldest(blockId, block);
          // Remove from cache
          super.removeEldest(blockId, block);
        }
      };
    } else if (maxCacheSize > 0) {
      // Capacity is calcuated by dividing the available space by estimated
      // space per
      // entry
      long maxCacheEntries = maxCacheSize / estimatedSpacePerCacheEntry;
      fileCache = new LRUCache<Long, byte[]>(maxCacheEntries) {

        @Override
        protected void removeEldest(Long blockId, byte[] block) {
          onRemoveEldest(blockId, block);
          // Remove from cache
          super.removeEldest(blockId, block);
        }

      };
    } else {
      fileCache = null;
    }
  }

  public RandomAccessRowFile(String storageFilePath, long fileId, int rowLength, long maxCacheSize,
          int blockSize, int recyclerCapacity) {
    this(storageFilePath, fileId, rowLength, maxCacheSize, null, null, blockSize, recyclerCapacity);
  }

  public RandomAccessRowFile(String storageFilePath, long fileId, int rowLength,
          SharedSpaceManager cacheSpaceManager, SharedSpaceConsumer cacheSpaceConsumer,
          int blockSize, int recyclerCapacity) {
    this(storageFilePath, fileId, rowLength, -1, cacheSpaceManager, cacheSpaceConsumer, blockSize,
            recyclerCapacity);
  }

  private void onRemoveEldest(Long blockId, byte[] block) {
    boolean dirty = block[dataLength] == 1;
    if (dirty) {
      // Persist entry before removing
      long start = System.nanoTime();
      try {
        writeBlockToFile(blockId, block, true);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (StatisticsDBTest.SUBBENCHMARKS) {
        SubbenchmarkManager.getInstance().addFileTime(fileId,
                SubbenchmarkManager.FileSubbenchmarkTask.RARF_DISKFLUSH, System.nanoTime() - start);
      }
      block[dataLength] = 0;
    }
    if (StatisticsDBTest.ENABLE_STORAGE_LOGGING) {
      uncachedBlocks.add(blockId);
      StorageLogWriter.getInstance().logBlockFlushEvent(fileId, blockId,
              fileCache.size() * estimatedSpacePerCacheEntry, getPercentageCached(), dirty);
    }
    if (RandomAccessRowFile.this.recyclerCapacity > 0) {
      blockRecycler.dump(block);
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
   *          The RandomAccessFile that will be read
   * @param rowId
   *          The row number in the file
   * @param rowLength
   *          The length of a row in the specified file
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
        if (StatisticsDBTest.ENABLE_STORAGE_LOGGING) {
          throw new UnsupportedOperationException(
                  "Storage logging is not yet implemented for rowsAsBlocks");
        }
        return readBlock(rowId);
      }
      long start = System.nanoTime();
      long blockId = rowId / rowsPerBlock;
      int blockOffset = (int) (rowId % rowsPerBlock) * rowLength;

      // byte[] block = readBlock(blockId);
      boolean cacheHit, blockFound;
      long start2 = System.nanoTime();
      byte[] block = fileCache.get(blockId);
      if (StatisticsDBTest.SUBBENCHMARKS) {
        SubbenchmarkManager.getInstance().addFileTime(fileId,
                SubbenchmarkManager.FileSubbenchmarkTask.RARF_CACHING, System.nanoTime() - start2);
      }
      if (block == null) {
        cacheHit = false;
        long start3 = System.nanoTime();
        block = readBlockFromFile(blockId, true);
        if (StatisticsDBTest.SUBBENCHMARKS) {
          SubbenchmarkManager.getInstance().addFileTime(fileId,
                  SubbenchmarkManager.FileSubbenchmarkTask.RARF_DISKREAD,
                  System.nanoTime() - start3);
        }
        if (StatisticsDBTest.ENABLE_STORAGE_LOGGING) {
          uncachedBlocks.remove(blockId);
        }
        if (block != null) {
          blockFound = true;
          cacheMisses++;
          long start4 = System.nanoTime();
          fileCache.put(blockId, block);
          if (StatisticsDBTest.SUBBENCHMARKS) {
            SubbenchmarkManager.getInstance().addFileTime(fileId,
                    SubbenchmarkManager.FileSubbenchmarkTask.RARF_CACHING,
                    System.nanoTime() - start4);
          }
        } else {
          blockFound = false;
          notExisting++;
        }
      } else {
        cacheHit = true;
        blockFound = true;
        cacheHits++;
      }

      if (block == null) {
        onReadRowFinished(start, blockId, cacheHit, false);
        return null;
      }
      byte[] row = new byte[rowLength];
      System.arraycopy(block, blockOffset, row, 0, rowLength);
      onReadRowFinished(start, blockId, cacheHit, blockFound && !Utils.isArrayZero(row));
      return row;
    } else {
      return readRowFromFile(rowId);
    }

  }

  /**
   * Is called when readRow finishes and transmits meta data of this operation
   * for statistical evaluations to the StorageLog or CentralLogger.
   */
  private void onReadRowFinished(long startTime, long blockId, boolean cacheHit, boolean found) {
    long time = System.nanoTime() - startTime;
    if (StatisticsDBTest.ENABLE_STORAGE_LOGGING) {
      StorageLogWriter.getInstance().logAccessEvent(fileId, blockId, false, true,
              fileCache.size() * estimatedSpacePerCacheEntry, getPercentageCached(), getFileSize(),
              cacheHit, found, time);
    }
    SubbenchmarkManager.getInstance().addFileOperationTime(fileId, time);
    if (StatisticsDBTest.SUBBENCHMARKS) {
      SubbenchmarkManager.getInstance().addFileTime(fileId,
              SubbenchmarkManager.FileSubbenchmarkTask.RARF, time);
    }
  }

  private byte[] readBlock(long blockId) throws IOException {
    byte[] block = fileCache.get(blockId);
    if (block == null) {
      long start = System.nanoTime();
      block = readBlockFromFile(blockId, true);
      if (StatisticsDBTest.SUBBENCHMARKS) {
        SubbenchmarkManager.getInstance().addFileTime(fileId,
                SubbenchmarkManager.FileSubbenchmarkTask.RARF_DISKREAD, System.nanoTime() - start);
      }
      if (block != null) {
        cacheMisses++;
        long start2 = System.nanoTime();
        fileCache.put(blockId, block);
        if (StatisticsDBTest.SUBBENCHMARKS) {
          SubbenchmarkManager.getInstance().addFileTime(fileId,
                  SubbenchmarkManager.FileSubbenchmarkTask.RARF_CACHING,
                  System.nanoTime() - start2);
        }
      } else {
        notExisting++;
      }
    } else {
      cacheHits++;
    }
    return block;
  }

  @SuppressWarnings("unused")
  private byte[] readBlockFromFile(long blockId, boolean logTime) throws IOException {
    if (!valid()) {
      throw new IllegalStateException("FileId " + fileId + ": Cannot operate on a closed storage");
    }
    long offset = blockId * dataLength;
    if (offset >= fileLength) {
      // Resource doesn't have an entry
      return null;
    }
    long start = System.nanoTime();
    rowFile.seek(offset);
    if (StatisticsDBTest.SUBBENCHMARKS && logTime) {
      SubbenchmarkManager.getInstance().addFileTime(fileId,
              SubbenchmarkManager.FileSubbenchmarkTask.RARF_RAWDISKREAD, System.nanoTime() - start);
    }

    // Initialize block array
    byte[] block = null;
    if (recyclerCapacity > 0) {
      block = blockRecycler.retrieve();
    }
    if (block == null) {
      block = new byte[cacheBlockSize];
    }

    start = System.nanoTime();
    int bytesRead = rowFile.read(block, 0, dataLength);
    if (StatisticsDBTest.SUBBENCHMARKS && logTime) {
      SubbenchmarkManager.getInstance().addFileTime(fileId,
              SubbenchmarkManager.FileSubbenchmarkTask.RARF_RAWDISKREAD, System.nanoTime() - start);
    }

    if (bytesRead != dataLength) {
      throw new RuntimeException("FileId " + fileId + ": Corrupted database: EOF before block end");
    }
    return block;
  }

  private byte[] readRowFromFile(long rowId) throws IOException {
    if (!valid()) {
      throw new IllegalStateException("FileId " + fileId + ": Cannot operate on a closed storage");
    }
    long offset = rowId * rowLength;
    if (offset >= fileLength) {
      // Resource doesn't have an entry
      return null;
    }
    rowFile.seek(offset);
    byte[] row = new byte[rowLength];
    try {
      rowFile.readFully(row);
    } catch (EOFException e) {
      throw new RuntimeException("FileId " + fileId + ": Corrupted database: EOF before row end");
    }
    return row;
  }

  /**
   * Writes a row into a {@link RandomAccessFile}. The offset is calculated by
   * <code>rowId * row.length</code>.
   *
   * @param rowFile
   *          A RandomAccessFile that will be updated
   * @param rowId
   *          The number of the row that will be written
   * @param row
   *          The data for the row as byte array
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
      long start = System.nanoTime();
      // We don't handle the rowsAsBlocks case separately because the procedure
      // would
      // be almost identical, because
      // we need to extend the row array for the dirty flag
      long blockId = rowId / rowsPerBlock;
      int blockOffset = (int) (rowId % rowsPerBlock) * rowLength;

      // byte[] block = readBlock(blockId);
      boolean cacheHit, blockFound;
      long start2 = System.nanoTime();
      byte[] block = fileCache.get(blockId);
      if (StatisticsDBTest.SUBBENCHMARKS) {
        SubbenchmarkManager.getInstance().addFileTime(fileId,
                SubbenchmarkManager.FileSubbenchmarkTask.RARF_CACHING, System.nanoTime() - start2);
      }
      if (block == null) {
        cacheHit = false;
        long start3 = System.nanoTime();
        block = readBlockFromFile(blockId, true);
        if (StatisticsDBTest.SUBBENCHMARKS) {
          SubbenchmarkManager.getInstance().addFileTime(fileId,
                  SubbenchmarkManager.FileSubbenchmarkTask.RARF_DISKREAD,
                  System.nanoTime() - start3);
        }
        if (block != null) {
          blockFound = true;
          cacheMisses++;
          long start4 = System.nanoTime();
          fileCache.put(blockId, block);
          if (StatisticsDBTest.SUBBENCHMARKS) {
            SubbenchmarkManager.getInstance().addFileTime(fileId,
                    SubbenchmarkManager.FileSubbenchmarkTask.RARF_CACHING,
                    System.nanoTime() - start4);
          }
          if (StatisticsDBTest.ENABLE_STORAGE_LOGGING) {
            uncachedBlocks.remove(blockId);
          }
        } else {
          blockFound = false;
          notExisting++;
        }
      } else {
        cacheHit = true;
        blockFound = true;
        cacheHits++;
      }
      byte[] readBlock = null;
      if (StatisticsDBTest.ENABLE_STORAGE_LOGGING) {
        if (block != null) {
          readBlock = block.clone();
        }
      }

      if (block == null) {
        if (recyclerCapacity > 0) {
          block = blockRecycler.retrieve();
        }
        if (block == null) {
          block = new byte[cacheBlockSize];
        }
      }
      System.arraycopy(row, 0, block, blockOffset, row.length);
      long start5 = System.nanoTime();
      if (!cacheHit && !blockFound) {
        fileCache.put(blockId, block);
      }
      if (StatisticsDBTest.SUBBENCHMARKS) {
        SubbenchmarkManager.getInstance().addFileTime(fileId,
                SubbenchmarkManager.FileSubbenchmarkTask.RARF_CACHING, System.nanoTime() - start5);
      }
      // Set dirty flag (located right behind the data)
      block[dataLength] = 1;

      onWriteRowFinished(start, blockId, blockOffset, readBlock, cacheHit, blockFound);
    } else {
      writeRowToFile(rowId, row);
    }
    return true;
  }

  /**
   * Is called when writeRow finishes and transmits meta data of this operation
   * for statistical evaluations to the StorageLog or CentralLogger.
   */
  private void onWriteRowFinished(long startTime, long blockId, int blockOffset, byte[] readBlock,
          boolean cacheHit, boolean blockFound) {
    long time = System.nanoTime() - startTime;
    if (StatisticsDBTest.ENABLE_STORAGE_LOGGING) {
      byte[] readRow = new byte[cacheBlockSize];
      if (readBlock != null) {
        System.arraycopy(readBlock, blockOffset, readRow, 0, rowLength);
      }
      StorageLogWriter.getInstance().logAccessEvent(fileId, blockId, true, true,
              fileCache.size() * estimatedSpacePerCacheEntry, getPercentageCached(), getFileSize(),
              cacheHit, blockFound && !Utils.isArrayZero(readRow), time);
    }
    SubbenchmarkManager.getInstance().addFileOperationTime(fileId, time);
    if (StatisticsDBTest.SUBBENCHMARKS) {
      SubbenchmarkManager.getInstance().addFileTime(fileId,
              SubbenchmarkManager.FileSubbenchmarkTask.RARF, time);
    }
  }

  private void writeRowToFile(long rowId, byte[] row) throws IOException {
    long offset = rowId * rowLength;
    rowFile.seek(offset);
    rowFile.write(row);
    if ((offset + row.length) > fileLength) {
      fileLength = offset + row.length;
    }
  }

  @SuppressWarnings("unused")
  private void writeBlockToFile(long blockId, byte[] block, boolean logTime) throws IOException {
    long offset = blockId * dataLength;
    long start = System.nanoTime();
    rowFile.seek(offset);
    rowFile.write(block, 0, dataLength);
    if (StatisticsDBTest.SUBBENCHMARKS && logTime) {
      SubbenchmarkManager.getInstance().addFileTime(fileId,
              SubbenchmarkManager.FileSubbenchmarkTask.RARF_RAWDISKWRITE,
              System.nanoTime() - start);
    }
    if ((offset + dataLength) > fileLength) {
      fileLength = offset + dataLength;
    }
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
    return new Iterator<Entry<Long, byte[]>>() {

      long blockId = 0;

      @Override
      public boolean hasNext() {
        return ((blockId + 1) * dataLength) <= fileLength;
      }

      @Override
      public Entry<Long, byte[]> next() {
        if (!hasNext()) {
          throw new NoSuchElementException(
                  "FileId " + fileId + ": Use hasNext() before calling next()");
        }
        try {
          // We ignore the cache to prevent lots of cache updates (one per read)
          // and a not
          // optimal cache
          // content afterwards (the last few blocks would be stored, and not
          // the most
          // frequent used ones).
          byte[] block = readBlockFromFile(blockId, false);
          if (block == null) {
            // This should never happen because hasNext() checks for this
            throw new IOException("FileId " + fileId + ": Invalid file length");
          }
          if (block.length < dataLength) {
            throw new RuntimeException("FileId " + fileId + ": Block read is too short: "
                    + block.length + " but dataLength is " + dataLength);
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
    if (!valid()) {
      throw new IllegalStateException("FileId " + fileId + ": Cannot operate on a closed storage");
    }
    while (blocks.hasNext()) {
      Entry<Long, byte[]> blockEntry = blocks.next();
      byte[] block = blockEntry.getValue();
      writeBlockToFile(blockEntry.getKey(), block, false);
      if (StatisticsDBTest.ENABLE_STORAGE_LOGGING) {
        uncachedBlocks.add(blockEntry.getKey());
      }
    }
  }

  @Override
  public void flush() throws IOException {
    if ((fileCache != null) && (!fileCache.isEmpty())) {
      if (!valid()) {
        throw new IllegalStateException(
                "FileId " + fileId + ": Cannot operate on a closed storage");
      }
      for (Entry<Long, byte[]> entry : fileCache) {
        byte[] block = entry.getValue();
        writeBlockToFile(entry.getKey(), block, false);
        // Clear dirty flag
        block[dataLength] = 0;
      }
      if (recyclerCapacity > 0) {
        blockRecycler.printStats(String.valueOf(fileId));
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
    return file.length();
  }

  @Override
  public int getRowLength() {
    return rowLength;
  }

  public long[] getStorageStatistics() {
    return new long[] { cacheHits, cacheMisses, notExisting };
  }

  private byte getPercentageCached() {
    double percentage = (fileCache.size() / (double) (fileCache.size() + uncachedBlocks.size()))
            * 100;
    return (byte) Math.round(percentage);
  }

  private long getFileSize() {
    long fileSize = ((fileCache.size() * cacheBlockSize) + (uncachedBlocks.size() * dataLength));
    return fileSize;
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
    fileLength = 0;
  }

  @Override
  public void close() {
    try {
      flush();
      rowFile.close();
      if (fileCache != null) {
        // Clearing also ensures releasing of shared cache space
        fileCache.clear();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean makeRoom() {
    if ((fileCache == null) || fileCache.isEmpty()) {
      return false;
    }
    long start = System.nanoTime();
    fileCache.evict();
    if (StatisticsDBTest.SUBBENCHMARKS) {
      SubbenchmarkManager.getInstance().addFileTime(fileId,
              SubbenchmarkManager.FileSubbenchmarkTask.RARF_CACHING, System.nanoTime() - start);
    }
    return true;
  }

  @Override
  public boolean isAbleToMakeRoomForOwnRequests() {
    return true;
  }

  @Override
  public long accessCosts() {
    return file.length();
  }

}
