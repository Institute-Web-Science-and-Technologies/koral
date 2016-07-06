package de.uni_koblenz.west.koral.master.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.NoSuchElementException;

/**
 * An iterator over long values read from a file created by
 * {@link SingleFileAdjacencyMatrix}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class SingleFileAdjacencyMatrixLongIterator implements LongIterator {

  private static final int NUMBER_OF_ENTRIES_IN_MEMORY = 1024 * 1024;

  private static final int SIZE_TILL_MAPPING_IS_APPLIED = 1024 * 1024;

  private final RandomAccessFile adjacencyMatrix;

  private long nextOffset;

  private final long size;

  private MappedByteBuffer currentPage;

  private long firstByteInPageOffset;

  private final long lengthOfPage;

  public SingleFileAdjacencyMatrixLongIterator(File adjacencyMatrixFile,
          long offsetOfAdjacencyListHead, long sizeOfAdjacencyList) {
    try {
      adjacencyMatrix = new RandomAccessFile(adjacencyMatrixFile, "r");
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
    size = sizeOfAdjacencyList;
    nextOffset = offsetOfAdjacencyListHead;
    if (isMemoryMappingApplied()) {
      lengthOfPage = size < SingleFileAdjacencyMatrixLongIterator.NUMBER_OF_ENTRIES_IN_MEMORY
              ? size * 2 * Long.BYTES
              : SingleFileAdjacencyMatrixLongIterator.NUMBER_OF_ENTRIES_IN_MEMORY * 2 * Long.BYTES;
      mapPageInMemory();
    } else {
      lengthOfPage = -1;
    }
  }

  private boolean isMemoryMappingApplied() {
    return size() < SingleFileAdjacencyMatrixLongIterator.SIZE_TILL_MAPPING_IS_APPLIED;
  }

  private void mapPageInMemory() {
    try {
      if (hasNext() && ((currentPage == null) || (nextOffset < firstByteInPageOffset)
              || (nextOffset >= (firstByteInPageOffset + lengthOfPage)))) {
        firstByteInPageOffset = Math.max(0, (nextOffset - lengthOfPage) + (2 * Long.BYTES));
        currentPage = adjacencyMatrix.getChannel().map(MapMode.READ_ONLY, firstByteInPageOffset,
                lengthOfPage);
      }
    } catch (IOException e) {
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
      if (isMemoryMappingApplied()) {
        mapPageInMemory();
        currentPage.position((int) (nextOffset - firstByteInPageOffset));
        long adjacency = currentPage.getLong();
        nextOffset = currentPage.getLong();
        return adjacency;
      } else {
        adjacencyMatrix.seek(nextOffset);
        long adjacency = adjacencyMatrix.readLong();
        nextOffset = adjacencyMatrix.readLong();
        return adjacency;
      }
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
