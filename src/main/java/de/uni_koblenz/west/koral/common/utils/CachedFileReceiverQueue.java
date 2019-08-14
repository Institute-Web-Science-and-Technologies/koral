/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License
 * along with Koral.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
package de.uni_koblenz.west.koral.common.utils;

import de.uni_koblenz.west.koral.common.query.Mapping;
import de.uni_koblenz.west.koral.common.query.MappingRecycleCache;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Caches received mappings until a limit is reached. Thereafter, mappings are
 * written to files.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class CachedFileReceiverQueue implements Closeable {

  private final int maxCacheSize;

  private final File cacheDirectory;

  private final byte[][] messageCache;

  private final int[] firstIndexCache;

  private final int[] lengthCache;

  private int nextWriteIndex;

  private int nextReadIndex;

  private final File fileBuffer1;

  private DataOutputStream fileOutput1;

  private DataInputStream fileInput1;

  private final File fileBuffer2;

  private DataOutputStream fileOutput2;

  private DataInputStream fileInput2;

  private long size;

  private QueueStatus status;

  public CachedFileReceiverQueue(int maxCacheSize, File cacheDirectory, int queueId) {
    this.maxCacheSize = maxCacheSize;
    this.cacheDirectory = cacheDirectory;
    messageCache = new byte[this.maxCacheSize][];
    firstIndexCache = new int[this.maxCacheSize];
    lengthCache = new int[this.maxCacheSize];
    nextReadIndex = -1;
    nextWriteIndex = 0;
    if (!this.cacheDirectory.exists()) {
      this.cacheDirectory.mkdirs();
    }
    fileBuffer1 = new File(this.cacheDirectory.getAbsolutePath() + File.separatorChar + "queue"
            + queueId + "buffer1");
    fileBuffer2 = new File(this.cacheDirectory.getAbsolutePath() + File.separatorChar + "queue"
            + queueId + "buffer2");
    status = QueueStatus.MEMORY_MEMORY;
    size = 0;
  }

  public synchronized boolean isEmpty() {
    return size <= 0;
  }

  public synchronized long size() {
    return size;
  }

  private void enqueueInMemory(byte[] message, int firstIndex, int length) {
    if (!status.name().startsWith("MEMORY_")) {
      throw new IllegalStateException(
              "Illegal attempt to write to memory while being in state " + status.name());
    }
    if (isMemoryFull()) {
      throw new RuntimeException(
              "Enqueuing in memory not possible because memory cache has reached its limit.");
    }
    messageCache[nextWriteIndex] = message;
    firstIndexCache[nextWriteIndex] = firstIndex;
    lengthCache[nextWriteIndex] = length;
    if (nextReadIndex == -1) {
      // this was the first written entry.
      nextReadIndex = nextWriteIndex;
    }
    nextWriteIndex = (nextWriteIndex + 1) % maxCacheSize;
    if (isMemoryFull()) {
      switch (status) {
        case MEMORY_MEMORY:
          status = QueueStatus.FILE1_MEMORY;
          break;
        case MEMORY_FILE1:
          status = QueueStatus.FILE2_FILE1;
          break;
        case MEMORY_FILE2:
          status = QueueStatus.FILE1_FILE2;
          break;
        default:
          break;
      }
    }
  }

  private boolean isMemoryFull() {
    if (nextReadIndex == -1) {
      return nextWriteIndex == maxCacheSize;
    } else {
      return nextWriteIndex == nextReadIndex;
    }
  }

  private Mapping dequeueFromMemory(MappingRecycleCache recycleCache) throws IOException {
    if (!status.name().endsWith("_MEMORY")) {
      throw new IllegalStateException(
              "Illegal attempt to read from memory while being in state " + status.name());
    }
    if (isMemoryEmpty()) {
      // this cache is empty
      return null;
    }
    Mapping result = recycleCache.createMapping(messageCache[nextReadIndex],
            firstIndexCache[nextReadIndex], lengthCache[nextReadIndex]);
    messageCache[nextReadIndex] = null;
    firstIndexCache[nextReadIndex] = -1;
    lengthCache[nextReadIndex] = -1;
    nextReadIndex = (nextReadIndex + 1) % maxCacheSize;
    if (nextReadIndex == nextWriteIndex) {
      nextReadIndex = -1;
    }
    if (isMemoryEmpty()) {
      switch (status) {
        case MEMORY_MEMORY:
          // there is nothing to do here
          break;
        case FILE1_MEMORY:
          status = QueueStatus.MEMORY_FILE1;
          if (fileOutput1 != null) {
            fileOutput1.close();
            fileOutput1 = null;
          }
          break;
        case FILE2_MEMORY:
          status = QueueStatus.MEMORY_FILE2;
          if (fileOutput2 != null) {
            fileOutput2.close();
            fileOutput2 = null;
          }
          break;
        default:
          break;
      }
    }
    return result;
  }

  private boolean isMemoryEmpty() {
    return nextReadIndex == -1;
  }

  private void enqueueInFile1(byte[] message, int firstIndex, int length) throws IOException {
    if (!status.name().startsWith("FILE1_")) {
      throw new IllegalStateException(
              "Illegal attempt to write to file1 while being in state " + status.name());
    }
    if (fileOutput1 == null) {
      fileOutput1 = new DataOutputStream(
              new BufferedOutputStream(new FileOutputStream(fileBuffer1)));
    }
    enqueueInFile(fileOutput1, message, firstIndex, length);
  }

  private void enqueueInFile2(byte[] message, int firstIndex, int length) throws IOException {
    if (!status.name().startsWith("FILE2_")) {
      throw new IllegalStateException(
              "Illegal attempt to write to file2 while being in state " + status.name());
    }
    if (fileOutput2 == null) {
      fileOutput2 = new DataOutputStream(
              new BufferedOutputStream(new FileOutputStream(fileBuffer2)));
    }
    enqueueInFile(fileOutput2, message, firstIndex, length);
  }

  private void enqueueInFile(DataOutputStream fileOutput, byte[] message, int firstIndex,
          int length) throws IOException {
    fileOutput.writeInt(length);
    fileOutput.write(message, firstIndex, length);
  }

  private Mapping dequeueFromFile1(MappingRecycleCache recycleCache) throws IOException {
    if (!status.name().endsWith("_FILE1")) {
      throw new IllegalStateException(
              "Illegal attempt to read from file1 while being in state " + status.name());
    }
    try {
      if (!fileBuffer1.exists()) {
        throw new EOFException();
      }
      if (fileInput1 == null) {
        fileInput1 = new DataInputStream(new BufferedInputStream(new FileInputStream(fileBuffer1)));
      }
      return dequeueFromFile(fileInput1, recycleCache);
    } catch (EOFException e) {
      // the file is empty
      if (fileInput1 != null) {
        fileInput1.close();
        fileInput1 = null;
      }
      new FileOutputStream(fileBuffer1).close();
      switch (status) {
        case MEMORY_FILE1:
          status = QueueStatus.MEMORY_MEMORY;
          return dequeueFromMemory(recycleCache);
        case FILE2_FILE1:
          status = QueueStatus.FILE2_MEMORY;
          return dequeueFromMemory(recycleCache);
        default:
          throw new IllegalStateException();
      }
    }
  }

  private Mapping dequeueFromFile2(MappingRecycleCache recycleCache) throws IOException {
    if (!status.name().endsWith("_FILE2")) {
      throw new IllegalStateException(
              "Illegal attempt to read from file2 while being in state " + status.name());
    }
    try {
      if (!fileBuffer2.exists()) {
        throw new EOFException();
      }
      if (fileInput2 == null) {
        fileInput2 = new DataInputStream(new BufferedInputStream(new FileInputStream(fileBuffer2)));
      }
      return dequeueFromFile(fileInput2, recycleCache);
    } catch (EOFException e) {
      // the file is empty
      if (fileInput2 != null) {
        fileInput2.close();
        fileInput2 = null;
      }
      new FileOutputStream(fileBuffer2).close();
      switch (status) {
        case MEMORY_FILE2:
          status = QueueStatus.MEMORY_MEMORY;
          return dequeueFromMemory(recycleCache);
        case FILE1_FILE2:
          status = QueueStatus.FILE1_MEMORY;
          return dequeueFromMemory(recycleCache);
        default:
          throw new IllegalStateException();
      }
    }
  }

  private Mapping dequeueFromFile(DataInputStream fileInput, MappingRecycleCache recycleCache)
          throws IOException {
    int length = fileInput.readInt();
    byte[] content = new byte[length];
    fileInput.readFully(content);
    return recycleCache.createMapping(content, 0, content.length);
  }

  public synchronized void enqueue(byte[] message, int firstIndex, int length) {
    try {
      switch (status) {
        case CLOSED:
          throw new IllegalStateException("Queue has already been closed.");
        case MEMORY_MEMORY:
        case MEMORY_FILE1:
        case MEMORY_FILE2:
          enqueueInMemory(message, firstIndex, length);
          break;
        case FILE1_MEMORY:
        case FILE1_FILE2:
          enqueueInFile1(message, firstIndex, length);
          break;
        case FILE2_MEMORY:
        case FILE2_FILE1:
          enqueueInFile2(message, firstIndex, length);
          break;
      }
      size++;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public synchronized Mapping dequeue(MappingRecycleCache recycleCache) {
    try {
      Mapping result = null;
      switch (status) {
        case CLOSED:
          throw new IllegalStateException("Queue has already been closed.");
        case MEMORY_MEMORY:
        case FILE1_MEMORY:
        case FILE2_MEMORY:
          result = dequeueFromMemory(recycleCache);
          break;
        case MEMORY_FILE1:
        case FILE2_FILE1:
          result = dequeueFromFile1(recycleCache);
          break;
        case MEMORY_FILE2:
        case FILE1_FILE2:
          result = dequeueFromFile2(recycleCache);
          break;
      }
      if (result != null) {
        size--;
      }
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public synchronized boolean isClosed() {
    return status == QueueStatus.CLOSED;
  }

  @Override
  public synchronized void close() {
    status = QueueStatus.CLOSED;
    try {
      if (fileInput1 != null) {
        fileInput1.close();
      } else if (fileOutput1 != null) {
        fileOutput1.close();
      }
      if (fileInput2 != null) {
        fileInput2.close();
      } else if (fileOutput2 != null) {
        fileOutput2.close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      deleteCacheDirectory();
    }
  }

  private void deleteCacheDirectory() {
    if (!cacheDirectory.exists()) {
      return;
    }
    fileBuffer1.delete();
    fileBuffer2.delete();
    cacheDirectory.delete();
  }

}

/**
 * Defines the state of this queue. Each states consist of two letters:
 * <ol>
 * <li>the cache written to</li>
 * <li>the cache read from</li>
 * </ol>
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
enum QueueStatus {

  /**
   * memory is full =&gt; FILE1_MEMORY
   */
  MEMORY_MEMORY,

  /**
   * memory is empty =&gt; MEMORY_FILE1
   */
  FILE1_MEMORY,

  /**
   * file1 is empty =&gt; MEMORY_MEMORY <br>
   * memory is full =&gt; FILE2_FILE1
   */
  MEMORY_FILE1,

  /**
   * file1 is empty =&gt; FILE2_MEMORY
   */
  FILE2_FILE1,

  /**
   * memory is empty =&gt; MEMORY_FILE2
   */
  FILE2_MEMORY,

  /**
   * file2 is empty =&gt; MEMORY_MEMORY<br>
   * memory is full =&gt; FILE1_FILE2
   */
  MEMORY_FILE2,

  /**
   * file2 is empty =&gt; FILE1_MEMORY
   */
  FILE1_FILE2,

  CLOSED;

}