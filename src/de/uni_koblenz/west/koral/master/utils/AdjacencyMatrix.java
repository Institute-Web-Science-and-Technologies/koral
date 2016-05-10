package de.uni_koblenz.west.koral.master.utils;

import java.io.Closeable;
import java.io.File;
import java.util.HashSet;
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

  // TODO use most frequently used cache (max open files = 100)

  public AdjacencyMatrix(File workingDir) {
    this.workingDir = workingDir;
    areDuplicatesRemoved = true;
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
    adjacencyList1.close();
    FileLongSet adjacencyList2 = getInternalAdjacencyList(vertex2);
    adjacencyList2.append(vertex1);
    adjacencyList2.close();
  }

  public LongIterator getAdjacencyList(long vertex) {
    if (!areDuplicatesRemoved) {
      removeDuplicates();
    }
    return getInternalAdjacencyList(vertex).iterator();
  }

  private FileLongSet getInternalAdjacencyList(long vertex) {
    return new FileLongSet(new File(workingDir.getAbsolutePath() + File.separator + vertex));
  }

  private void removeDuplicates() {
    for (long vertex = 1; vertex <= numberOfVertices; vertex++) {
      FileLongSet adjacencyList = getInternalAdjacencyList(vertex);
      File newFile = new File(adjacencyList.getFile().getAbsolutePath() + "_copy");
      adjacencyList.getFile().renameTo(newFile);
      adjacencyList = new FileLongSet(newFile);
      FileLongSet newAdjacencyList = getInternalAdjacencyList(vertex);
      if (newFile.length() < (1024 * 1024)) {
        // duplicates can be checked in memory
        Set<Long> alreadySeen = new HashSet<>();
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
      } else {
        // duplicate have to be detected on disk
        LongIterator iterator = adjacencyList.iterator();
        while (iterator.hasNext()) {
          long next = iterator.next();
          boolean isNew = newAdjacencyList.add(next);
          if (isNew) {
            numberOfEdges++;
          }
        }
        iterator.close();
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
    // TODO Auto-generated method stub

  }

}
