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

  private final boolean removeDuplicates;

  protected final File workingDir;

  private long numberOfVertices;

  private long numberOfEdges;

  private boolean areDuplicatesRemoved;

  public AdjacencyMatrix(File workingDir, boolean removeDuplicates) {
    this.removeDuplicates = removeDuplicates;
    this.workingDir = workingDir;
    areDuplicatesRemoved = true;
  }

  protected boolean isRemoveDuplicatesSet() {
    return removeDuplicates;
  }

  public long getNumberOfVertices() {
    return numberOfVertices;
  }

  public long getNumberOfEdges() {
    if (!areDuplicatesRemoved && removeDuplicates) {
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
    if (!removeDuplicates) {
      numberOfEdges++;
    }
    addAdjacentVertex(vertex1, vertex2);
    addAdjacentVertex(vertex2, vertex1);
  }

  private void addAdjacentVertex(long vertex, long adjacency) {
    AdjacencyList adjacencyList = getInternalAdjacencyList(vertex);
    adjacencyList.append(adjacency);
  }

  public void addEdge(long vertex1, long vertex2, long edgeWeight) {
    areDuplicatesRemoved = false;
    if (vertex1 > numberOfVertices) {
      numberOfVertices = vertex1;
    }
    if (vertex2 > numberOfVertices) {
      numberOfVertices = vertex2;
    }
    if (!removeDuplicates) {
      numberOfEdges++;
    }
    addAdjacentVertex(vertex1, vertex2, edgeWeight);
    addAdjacentVertex(vertex2, vertex1, edgeWeight);
  }

  private void addAdjacentVertex(long vertex, long adjacency, long edgeWeight) {
    AdjacencyList adjacencyList = getInternalAdjacencyList(vertex);
    adjacencyList.append(adjacency, edgeWeight);
  }

  public LongIterator getAdjacencyList(long vertex) {
    if (!areDuplicatesRemoved && removeDuplicates) {
      removeDuplicates();
    }
    return getInternalAdjacencyList(vertex).iterator();
  }

  public LongArrayIterator getWeightedAdjacencyList(long vertex) {
    if (!areDuplicatesRemoved && removeDuplicates) {
      removeDuplicates();
    }
    return getInternalAdjacencyList(vertex).iteratorForArray();
  }

  protected abstract AdjacencyList getInternalAdjacencyList(long vertex);

  private void removeDuplicates() {
    numberOfEdges = removeDuplicatesFromAdjacencyLists(numberOfVertices);
    numberOfEdges /= 2;
    areDuplicatesRemoved = true;
  }

  /**
   * @param numberOfVertices
   * @return number of directed edges
   */
  protected abstract long removeDuplicatesFromAdjacencyLists(long numberOfVertices);

  @Override
  public void close() {
  }

}
