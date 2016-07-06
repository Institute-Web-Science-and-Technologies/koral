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
