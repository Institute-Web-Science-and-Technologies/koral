package de.uni_koblenz.west.cidre.master.statisticsDB;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.cidre.master.statisticsDB.impl.MapDBGraphStatisticsDatabase;

import java.io.Closeable;
import java.util.BitSet;
import java.util.logging.Logger;

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

    // check load of the different chunks
    statistics = database.getOwnerLoad();
    argMin(statistics, 0, ownerCandidates);
    assert ownerCandidates.size() > 0;
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

  private void argMin(long[] statistics, int offset, BitSet ownerCandidates) {
    long min = Long.MAX_VALUE;
    int currentPos = ownerCandidates.nextSetBit(0);
    while (currentPos > -1) {
      if (statistics[offset + currentPos] > min) {
        ownerCandidates.clear(currentPos);
      } else if (statistics[offset + currentPos] < min) {
        min = statistics[offset + currentPos];
        ownerCandidates.set(0, currentPos, false);
      }
      currentPos = ownerCandidates.nextSetBit(currentPos + 1);
    }
  }

  /**
   * updates the dictionary such that the first two bytes of id is set to owner.
   * 
   * @param id
   * @param owner
   * @return
   * @throws IllegalArgumentException
   *           if the first two bytes of id are not 0 or not equal to owner
   */
  public long setOwner(long id, short owner) {
    return database.setOwner(id, owner);
  }

  public long getSubjectFrequency(long subject, int slave) {
    long[] statisticsForResource = database.getStatisticsForResource(subject);
    if (statisticsForResource == null) {
      // this resource does not occur
      return 0;
    }
    return statisticsForResource[0 * numberOfChunks + slave];
  }

  public long getPropertyFrequency(long property, int slave) {
    long[] statisticsForResource = database.getStatisticsForResource(property);
    if (statisticsForResource == null) {
      // this resource does not occur
      return 0;
    }
    return statisticsForResource[1 * numberOfChunks + slave];
  }

  public long getObjectFrequency(long object, int slave) {
    long[] statisticsForResource = database.getStatisticsForResource(object);
    if (statisticsForResource == null) {
      // this resource does not occur
      return 0;
    }
    return statisticsForResource[2 * numberOfChunks + slave];
  }

  public long getOwnerLoad(int slave) {
    return database.getOwnerLoad()[slave];
  }

  public long getTotalSubjectFrequency(long subject) {
    long totalFrequency = 0;
    long[] statisticsForResource = database.getStatisticsForResource(subject);
    if (statisticsForResource == null) {
      // this resource does not occur
      return 0;
    }
    for (int slave = 0; slave < numberOfChunks; slave++) {
      totalFrequency += statisticsForResource[0 * numberOfChunks + slave];
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
      totalFrequency += statisticsForResource[1 * numberOfChunks + slave];
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
      totalFrequency += statisticsForResource[2 * numberOfChunks + slave];
    }
    return totalFrequency;
  }

  public long getTotalOwnerLoad() {
    long result = 0;
    long[] ownerloads = database.getOwnerLoad();
    for (long load : ownerloads) {
      result += load;
    }
    return result;
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
