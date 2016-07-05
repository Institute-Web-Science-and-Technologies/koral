package de.uni_koblenz.west.koral.master.utils;

import java.io.Closeable;
import java.io.File;

/**
 * A persistent adjacency matrix.
 *
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public abstract class AdjacencyMatrix implements Closeable {

  protected final File workingDir;

  private long numberOfVertices;

  private long numberOfEdges;

  private boolean areDuplicatesRemoved;

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
    addAdjacentVertex(vertex1, vertex2);
    addAdjacentVertex(vertex2, vertex1);
  }

  public LongIterator getAdjacencyList(long vertex) {
    if (!areDuplicatesRemoved) {
      removeDuplicates();
    }
    return getInternalAdjacencyList(vertex).iterator();
  }

  private void addAdjacentVertex(long vertex, long adjacency) {
    AdjacencyList adjacencyList = getInternalAdjacencyList(vertex);
    adjacencyList.append(adjacency);
  }

  protected abstract AdjacencyList getInternalAdjacencyList(long vertex);

  private void removeDuplicates() {
    numberOfEdges = removeDuplicatesFromAdjacencyLists(numberOfVertices);
    numberOfEdges /= 2;
    areDuplicatesRemoved = true;
  }

  protected abstract long removeDuplicatesFromAdjacencyLists(long numberOfVertices);

  @Override
  public void close() {
  }

}
