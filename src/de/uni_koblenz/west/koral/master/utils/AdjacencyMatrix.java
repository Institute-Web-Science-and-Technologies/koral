package de.uni_koblenz.west.koral.master.utils;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;

import java.io.Closeable;
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * A persistent adjacency matrix.
 *
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class AdjacencyMatrix implements Closeable {

  private final File workingDir;

  private long numberOfVertices;

  private long numberOfEdges;

  private boolean areDuplicatesRemoved;

  private final Queue<Long> lruCache;

  private final Map<Long, FileLongSet> vertex2adjacentList;

  public AdjacencyMatrix(File workingDir) {
    this.workingDir = workingDir;
    areDuplicatesRemoved = true;
    lruCache = new LinkedList<>();
    vertex2adjacentList = new HashMap<>();
  }

  public long getNumberOfVertices() {
    return numberOfVertices;
  }

  public long getNumberOfEdges() {
    if (!areDuplicatesRemoved) {
      removeDuplicates();
    }
    return numberOfEdges;
  }

  public void addEdge(long vertex1, long vertex2) {
    areDuplicatesRemoved = false;
    if (vertex1 > numberOfVertices) {
      numberOfVertices = vertex1;
    }
    if (vertex2 > numberOfVertices) {
      numberOfVertices = vertex2;
    }
    FileLongSet adjacencyList1 = getInternalAdjacencyList(vertex1);
    adjacencyList1.append(vertex2);
    FileLongSet adjacencyList2 = getInternalAdjacencyList(vertex2);
    adjacencyList2.append(vertex1);
  }

  public LongIterator getAdjacencyList(long vertex) {
    if (!areDuplicatesRemoved) {
      removeDuplicates();
    }
    return getInternalAdjacencyList(vertex).iterator();
  }

  private FileLongSet getInternalAdjacencyList(long vertex) {
    FileLongSet set = vertex2adjacentList.get(vertex);
    if (set == null) {
      set = new FileLongSet(new File(workingDir.getAbsolutePath() + File.separator + vertex));
      if (lruCache.size() == 100) {
        long oldestVertex = lruCache.poll();
        FileLongSet toBeRemoved = vertex2adjacentList.remove(oldestVertex);
        toBeRemoved.close();
      }
      vertex2adjacentList.put(vertex, set);
    } else {
      lruCache.remove(vertex);
    }
    lruCache.offer(vertex);
    return set;
  }

  private void removeDuplicates() {
    DBMaker<?> dbmaker = MapDBStorageOptions.MEMORY_MAPPED_FILE
            .getDBMaker(workingDir.getAbsolutePath() + File.separator + "adjacencySets")
            .transactionDisable().closeOnJvmShutdown().asyncWriteEnable();
    dbmaker = MapDBCacheOptions.HASH_TABLE.setCaching(dbmaker);
    DB database = dbmaker.make();
    Set<Long> vertexSet = database.createHashSet("adjacencys").makeOrGet();

    for (long vertex = 1; vertex <= numberOfVertices; vertex++) {
      FileLongSet adjacencyList = getInternalAdjacencyList(vertex);
      adjacencyList.close();
      File newFile = new File(adjacencyList.getFile().getAbsolutePath() + "_copy");
      adjacencyList.getFile().renameTo(newFile);
      adjacencyList = new FileLongSet(newFile);
      FileLongSet newAdjacencyList = getInternalAdjacencyList(vertex);
      boolean isSmall = newFile.length() < (1024 * 1024);
      // duplicates can be checked in memory
      Set<Long> alreadySeen = isSmall ? new HashSet<>() : vertexSet;
      LongIterator iterator = adjacencyList.iterator();
      while (iterator.hasNext()) {
        long next = iterator.next();
        boolean isNew = alreadySeen.add(next);
        if (isNew) {
          newAdjacencyList.append(next);
          numberOfEdges++;
        }
      }
      iterator.close();
      if (!isSmall) {
        vertexSet.clear();
      }
      newAdjacencyList.close();
      adjacencyList.close();
      newFile.delete();
    }
    numberOfEdges /= 2;
    areDuplicatesRemoved = true;
  }

  @Override
  public void close() {
    for (FileLongSet set : vertex2adjacentList.values()) {
      set.close();
    }
  }

}
