package de.uni_koblenz.west.koral.master.statisticsDB.impl;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatisticsDatabase;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * {@link GraphStatisticsDatabase} realized via a random access file.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class SingleFileGraphStatisticsDatabase implements GraphStatisticsDatabase {

  private RandomAccessFile statistics;

  private final File statisticsFile;

  private final short numberOfChunks;

  public SingleFileGraphStatisticsDatabase(String statisticsDir, short numberOfChunks) {
    File statisticsDirFile = new File(statisticsDir);
    if (!statisticsDirFile.exists()) {
      statisticsDirFile.mkdirs();
    }
    this.numberOfChunks = numberOfChunks;

    statisticsFile = new File(statisticsDirFile.getAbsolutePath() + File.separator + "statistics");
    createStatistics();
  }

  private void createStatistics() {
    try {
      statistics = new RandomAccessFile(statisticsFile, "rw");
    } catch (IOException e) {
      close();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void incrementNumberOfTriplesPerChunk(int chunk) {
    try {
      // number of triples per chunk are stored at the beginning of the file
      long offset = chunk * Long.BYTES;
      statistics.seek(offset);
      try {
        long numberOfTriplesInChunk = statistics.readLong();
        numberOfTriplesInChunk++;
        statistics.seek(offset);
        statistics.writeLong(numberOfTriplesInChunk);
      } catch (EOFException e) {
        if (statistics.getFilePointer() != offset) {
          statistics.seek(offset);
        }
        statistics.writeLong(1);
      }
    } catch (IOException e) {
      close();
      throw new RuntimeException(e);
    }
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

  @Override
  public void incrementRessourceOccurrences(long resource, int chunk) {
    incrementValue(resource, 3 * numberOfChunks);
  }

  private void incrementValue(long resourceID, int column) {
    try {
      long sizeOfRow = (Long.BYTES * numberOfChunks * 3) + Long.BYTES;
      long sizeOfTriplesPerChunk = Long.BYTES * numberOfChunks;
      long offset = sizeOfTriplesPerChunk + ((resourceID - 1) * sizeOfRow) + (column * Long.BYTES);

      statistics.seek(offset);
      try {
        long value = statistics.readLong();
        value++;
        statistics.seek(offset);
        statistics.writeLong(value);
      } catch (EOFException e) {
        if (statistics.getFilePointer() != offset) {
          statistics.seek(offset);
        }
        statistics.writeLong(1);
      }
    } catch (IOException e) {
      close();
      throw new RuntimeException(e);
    }
  }

  @Override
  public long[] getChunkSizes() {
    try {
      statistics.seek(0);
      byte[] row = new byte[Long.BYTES * numberOfChunks];
      try {
        statistics.readFully(row);
      } catch (EOFException e) {
        // the statistics are not completes initialized
      }
      long[] result = new long[numberOfChunks];
      for (int i = 0; i < result.length; i++) {
        result[i] = NumberConversion.bytes2long(row, i * Long.BYTES);
      }
      return result;
    } catch (IOException e) {
      close();
      throw new RuntimeException(e);
    }
  }

  @Override
  public long[] getStatisticsForResource(long id) {
    try {
      int sizeOfRow = (Long.BYTES * numberOfChunks * 3) + Long.BYTES;
      long sizeOfTriplesPerChunk = Long.BYTES * numberOfChunks;
      long offset = sizeOfTriplesPerChunk + ((id - 1) * sizeOfRow);

      byte[] row = new byte[sizeOfRow];
      statistics.seek(offset);
      try {
        statistics.readFully(row);
      } catch (EOFException e) {
        // there exists no values yet
      }

      long[] result = new long[(numberOfChunks * 3) + 1];
      for (int i = 0; i < result.length; i++) {
        result[i] = NumberConversion.bytes2long(row, i * Long.BYTES);
      }
      return result;
    } catch (IOException e) {
      close();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clear() {
    close();
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
      int sizeOfRow = (Long.BYTES * numberOfChunks * 3) + Long.BYTES;
      long sizeOfTriplesPerChunk = Long.BYTES * numberOfChunks;
      long maxId = (statistics.length() - sizeOfTriplesPerChunk) / sizeOfRow;
      for (long id = 1; id <= maxId; id++) {
        sb.append("\n");
        sb.append(id);
        for (long value : getStatisticsForResource(id)) {
          sb.append(";").append(value);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return sb.toString();
  }

}
