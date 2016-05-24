package de.uni_koblenz.west.koral.master.statisticsDB;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.MapDBGraphStatisticsDatabase;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.BitSet;
import java.util.Random;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

/**
 * Stores statistical information about the occurrence of resources in the
 * different graph chunks. It receives its data from {@link DictionaryEncoder}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class GraphStatistics implements Closeable {

  @SuppressWarnings("unused")
  private final Logger logger;

  private final GraphStatisticsDatabase database;

  private final int numberOfChunks;

  public GraphStatistics(Configuration conf, short numberOfChunks, Logger logger) {
    this.logger = logger;
    this.numberOfChunks = numberOfChunks;
    database = new MapDBGraphStatisticsDatabase(conf.getStatisticsStorageType(),
            conf.getStatisticsDataStructure(), conf.getStatisticsDir(),
            conf.useTransactionsForStatistics(), conf.areStatisticsAsynchronouslyWritten(),
            conf.getStatisticsCacheType(), numberOfChunks);
  }

  public void collectStatistics(File[] encodedChunks) {
    clear();
    for (int i = 0; i < encodedChunks.length; i++) {
      collectStatistics(i, encodedChunks[i]);
    }
  }

  private void collectStatistics(int chunkIndex, File chunk) {
    if (chunk == null) {
      return;
    }
    try (DataInputStream in = new DataInputStream(
            new BufferedInputStream(new GZIPInputStream(new FileInputStream(chunk))));) {
      while (true) {
        long subject = in.readLong();
        long property = in.readLong();
        long object = in.readLong();
        short containmentSize = in.readShort();
        byte[] containment = new byte[containmentSize];
        in.readFully(containment);
        count(subject, property, object, chunkIndex);
      }
    } catch (EOFException e) {
      // the end of the file has been reached
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  public void count(long subject, long property, long object, int chunk) {
    database.incrementSubjectCount(subject, chunk);
    database.incrementRessourceOccurrences(subject, chunk);
    database.incrementPropertyCount(property, chunk);
    database.incrementRessourceOccurrences(property, chunk);
    database.incrementObjectCount(object, chunk);
    database.incrementRessourceOccurrences(object, chunk);
    database.incrementNumberOfTriplesPerChunk(chunk);
  }

  public long[] getChunkSizes() {
    return database.getChunkSizes();
  }

  public short getOwner(long id) {
    short owner = (short) (id >>> 48);
    if (owner != 0) {
      return owner;
    }
    long[] statistics = database.getStatisticsForResource(id);

    BitSet ownerCandidates = new BitSet(numberOfChunks);
    ownerCandidates.set(0, numberOfChunks, true);

    // check occurrence as subject
    argMax(statistics, 0 * numberOfChunks, ownerCandidates);
    assert ownerCandidates.size() > 0;
    if (ownerCandidates.size() == 1) {
      return (short) ownerCandidates.nextSetBit(0);
    }

    // check occurrence as object
    argMax(statistics, 2 * numberOfChunks, ownerCandidates);
    assert ownerCandidates.size() > 0;
    if (ownerCandidates.size() == 1) {
      return (short) ownerCandidates.nextSetBit(0);
    }

    // check occurrence as property
    argMax(statistics, 1 * numberOfChunks, ownerCandidates);
    assert ownerCandidates.size() > 0;
    if (ownerCandidates.size() == 1) {
      return (short) ownerCandidates.nextSetBit(0);
    }

    // pick a random owner
    pickRandom(statistics, 0, ownerCandidates);
    return (short) ownerCandidates.nextSetBit(0);
  }

  private void argMax(long[] statistics, int offset, BitSet ownerCandidates) {
    long max = Long.MIN_VALUE;
    int currentPos = ownerCandidates.nextSetBit(0);
    while (currentPos > -1) {
      if (statistics[offset + currentPos] < max) {
        ownerCandidates.clear(currentPos);
      } else if (statistics[offset + currentPos] > max) {
        max = statistics[offset + currentPos];
        ownerCandidates.set(0, currentPos, false);
      }
      currentPos = ownerCandidates.nextSetBit(currentPos + 1);
    }
  }

  private void pickRandom(long[] statistics, int offset, BitSet ownerCandidates) {
    Random rand = new Random(System.currentTimeMillis());
    while (ownerCandidates.cardinality() > 1) {
      if (rand.nextBoolean()) {
        // the first candidate is chosen
        break;
      }
      // eliminate candidate
      int currentPos = ownerCandidates.nextSetBit(0);
      ownerCandidates.clear(currentPos);
    }
  }

  public long getSubjectFrequency(long subject, int slave) {
    long[] statisticsForResource = database.getStatisticsForResource(subject);
    if (statisticsForResource == null) {
      // this resource does not occur
      return 0;
    }
    return statisticsForResource[(0 * numberOfChunks) + slave];
  }

  public long getPropertyFrequency(long property, int slave) {
    long[] statisticsForResource = database.getStatisticsForResource(property);
    if (statisticsForResource == null) {
      // this resource does not occur
      return 0;
    }
    return statisticsForResource[(1 * numberOfChunks) + slave];
  }

  public long getObjectFrequency(long object, int slave) {
    long[] statisticsForResource = database.getStatisticsForResource(object);
    if (statisticsForResource == null) {
      // this resource does not occur
      return 0;
    }
    return statisticsForResource[(2 * numberOfChunks) + slave];
  }

  public long getTotalSubjectFrequency(long subject) {
    long totalFrequency = 0;
    long[] statisticsForResource = database.getStatisticsForResource(subject);
    if (statisticsForResource == null) {
      // this resource does not occur
      return 0;
    }
    for (int slave = 0; slave < numberOfChunks; slave++) {
      totalFrequency += statisticsForResource[(0 * numberOfChunks) + slave];
    }
    return totalFrequency;
  }

  public long getTotalPropertyFrequency(long property) {
    long totalFrequency = 0;
    long[] statisticsForResource = database.getStatisticsForResource(property);
    if (statisticsForResource == null) {
      // this resource does not occur
      return 0;
    }
    for (int slave = 0; slave < numberOfChunks; slave++) {
      totalFrequency += statisticsForResource[(1 * numberOfChunks) + slave];
    }
    return totalFrequency;
  }

  public long getTotalObjectFrequency(long object) {
    long totalFrequency = 0;
    long[] statisticsForResource = database.getStatisticsForResource(object);
    if (statisticsForResource == null) {
      // this resource does not occur
      return 0;
    }
    for (int slave = 0; slave < numberOfChunks; slave++) {
      totalFrequency += statisticsForResource[(2 * numberOfChunks) + slave];
    }
    return totalFrequency;
  }

  public int getNumberOfChunks() {
    return numberOfChunks;
  }

  public void clear() {
    database.clear();
  }

  @Override
  public void close() {
    database.close();
  }

  @Override
  public String toString() {
    return database.toString();
  }

}
