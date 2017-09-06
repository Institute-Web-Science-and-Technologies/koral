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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.NoSuchElementException;

/**
 * An iterator over long values read from a file created by
 * {@link SingleFileAdjacencyMatrix}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class SingleFileAdjacencyMatrixLongIterator implements LongIterator {

  private final RandomAccessFile adjacencyMatrix;

  private long nextOffset;

  private final long size;

  public SingleFileAdjacencyMatrixLongIterator(File adjacencyMatrixFile,
          long offsetOfAdjacencyListHead, long sizeOfAdjacencyList) {
    size = sizeOfAdjacencyList;
    nextOffset = offsetOfAdjacencyListHead;
    try {
      adjacencyMatrix = new RandomAccessFile(adjacencyMatrixFile, "r");
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public long size() {
    return size;
  }

  @Override
  public boolean hasNext() {
    return nextOffset != SingleFileAdjacencyMatrix.TAIL_OFFSET_OF_LIST;
  }

  @Override
  public long next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    try {
      adjacencyMatrix.seek(nextOffset);
      long adjacency = adjacencyMatrix.readLong();
      nextOffset = adjacencyMatrix.readLong();
      return adjacency;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      adjacencyMatrix.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
