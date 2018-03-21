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
package de.uni_koblenz.west.koral.master.statisticsDB.impl;

import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatisticsDatabase;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

/**
 * {@link GraphStatisticsDatabase} realized via a memory mapped file.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MemoryMappedFileGraphStatisticsDatabase implements GraphStatisticsDatabase {

  private RandomAccessFile statistics;

  private final File statisticsFile;

  private final short numberOfChunks;

  private MappedByteBuffer triplesPerChunk;

  private MappedByteBuffer page;

  private long minId;

  private long maxId;

  private final long numberOfIds;

  public MemoryMappedFileGraphStatisticsDatabase(String statisticsDir, short numberOfChunks) {
    File statisticsDirFile = new File(statisticsDir);
    if (!statisticsDirFile.exists()) {
      statisticsDirFile.mkdirs();
    }
    this.numberOfChunks = numberOfChunks;
    numberOfIds = (100 * 1024 * 1024) / getRowSize();

    statisticsFile = new File(statisticsDirFile.getAbsolutePath() + File.separator + "statistics");
    createStatistics();
  }

  private void createStatistics() {
    try {
      statistics = new RandomAccessFile(statisticsFile, "rw");
      triplesPerChunk = getMappedBuffer(0, getSizeOfTriplesPerChunkRow());
    } catch (IOException e) {
      close();
      throw new RuntimeException(e);
    }
  }

  private MappedByteBuffer getMappedBuffer(long offset, long length) throws IOException {
    if ((offset + length) < statistics.length()) {
      statistics.seek((offset + length) - 1);
      statistics.writeByte(0);
    }
    return statistics.getChannel().map(MapMode.READ_WRITE, offset, length);
  }

  private int getSizeOfTriplesPerChunkRow() {
    return Long.BYTES * numberOfChunks;
  }

  private int getRowSize() {
    return (Long.BYTES * numberOfChunks * 3) + Long.BYTES;
  }

  @Override
  public void incrementNumberOfTriplesPerChunk(int chunk) {
    // number of triples per chunk are stored at the beginning of the file
    int offset = chunk * Long.BYTES;
    triplesPerChunk.position(offset);
    long numberOfTriples = triplesPerChunk.getLong();
    numberOfTriples++;
    triplesPerChunk.putLong(offset, numberOfTriples);
  }

  @Override
  public void incrementSubjectCount(long subject, int chunk) {
    incrementValue(subject, chunk);
  }

  @Override
  public void incrementPropertyCount(long property, int chunk) {
    incrementValue(property, numberOfChunks + chunk);
  }

  @Override
  public void incrementObjectCount(long object, int chunk) {
    incrementValue(object, (2 * numberOfChunks) + chunk);
  }

  public void incrementRessourceOccurrences(long resource, int chunk) {
    incrementValue(resource, 3 * numberOfChunks);
  }

  private void incrementValue(long resourceID, int column) {
    adjustPage(resourceID);
    int offset = (int) (((resourceID - minId) * getRowSize()) + (column * Long.BYTES));
    long value = page.getLong(offset);
    value++;
    page.putLong(offset, value);
  }

  private void adjustPage(long resourceID) {
    if ((resourceID < minId) || (resourceID > maxId)) {
      // the resourceID is not in the current page
      minId = (resourceID / numberOfIds) * numberOfIds;
      maxId = minId + numberOfIds;
      minId += 1;
      try {
        page = getMappedBuffer(getSizeOfTriplesPerChunkRow() + ((minId - 1) * getRowSize()),
                numberOfIds * getRowSize());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public long[] getChunkSizes() {
    long[] result = new long[numberOfChunks];
    for (int i = 0; i < result.length; i++) {
      result[i] = triplesPerChunk.getLong(i * Long.BYTES);
    }
    return result;
  }

  @Override
  public long[] getStatisticsForResource(long id) {
    if (id == 0) {
      return null;
    }
    adjustPage(id);
    int offset = (int) ((id - minId) * getRowSize());

    long[] result = new long[getRowSize() / Long.BYTES];
    for (int i = 0; i < result.length; i++) {
      result[i] = page.getLong(offset + (i * Long.BYTES));
    }
    return result;
  }

  @Override
  public void clear() {
    close();
    triplesPerChunk = null;
    page = null;
    minId = 0;
    maxId = 0;
    statisticsFile.delete();
    createStatistics();
  }

  @Override
  public void close() {
    try {
      statistics.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("TriplesPerChunk ");
    for (long l : getChunkSizes()) {
      sb.append("\t").append(l);
    }
    sb.append("\n");
    sb.append("ResourceID");
    for (int i = 0; i < numberOfChunks; i++) {
      sb.append(";").append("subjectInChunk").append(i);
    }
    for (int i = 0; i < numberOfChunks; i++) {
      sb.append(";").append("propertyInChunk").append(i);
    }
    for (int i = 0; i < numberOfChunks; i++) {
      sb.append(";").append("objectInChunk").append(i);
    }
    sb.append(";").append("overallOccurrance");
    try {
      int sizeOfRow = getRowSize();
      long sizeOfTriplesPerChunk = getSizeOfTriplesPerChunkRow();
      long maxId = (statistics.length() - sizeOfTriplesPerChunk) / sizeOfRow;
      for (long id = 1; id <= maxId; id++) {
        long[] statistics = getStatisticsForResource(id);
        boolean isEmpty = true;
        for (long value : statistics) {
          if (value != 0) {
            isEmpty = false;
            break;
          }
        }
        if (isEmpty) {
          break;
        }
        sb.append("\n");
        sb.append(id);
        for (long value : statistics) {
          sb.append(";").append(value);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return sb.toString();
  }

}
