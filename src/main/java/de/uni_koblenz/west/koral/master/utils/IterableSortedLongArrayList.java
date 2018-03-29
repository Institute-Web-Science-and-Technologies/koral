package de.uni_koblenz.west.koral.master.utils;

import de.uni_koblenz.west.koral.common.io.EncodedLongFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedLongFileOutputStream;
import de.uni_koblenz.west.koral.common.io.LongOutputWriter;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.NoSuchElementException;

/**
 * An iterable list of long arrays. Before iterating it is automatically sorted.
 * Duplicate elements might be removed before iterating it.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class IterableSortedLongArrayList implements AutoCloseable {

  private long size;

  private final long cacheSize;

  private final int arraySize;

  private long[][] cache;

  private int nextFreeIndex;

  private final Comparator<long[]> comparator;

  private final int maxNumberOfOpenFiles;

  private final File workingDir;

  private File listFile;

  private EncodedLongFileOutputStream output;

  private boolean isSorted;

  public IterableSortedLongArrayList(int arraySize, Comparator<long[]> comparator, long cacheSize,
          File workingDir, int maxNumberOfOpenFiles) {
    this.maxNumberOfOpenFiles = maxNumberOfOpenFiles;
    this.workingDir = workingDir;
    this.comparator = comparator;
    this.cacheSize = cacheSize;
    this.arraySize = arraySize;
    cache = new long[(int) (cacheSize / arraySize / Long.BYTES)][];
    nextFreeIndex = 0;
    isSorted = false;
    size = 0;
  }

  public void append(long... element) {
    if (element.length > arraySize) {
      throw new IllegalArgumentException("The element array has length " + element.length
              + " but must have a length of " + arraySize);
    }
    try {
      if ((cache != null) && (nextFreeIndex == cache.length)) {
        listFile = File.createTempFile("longArrayList", "", workingDir);
        output = new EncodedLongFileOutputStream(listFile);
        for (int i = 0; i < cache.length; i++) {
          for (long value : cache[i]) {
            output.writeLong(value);
          }
        }
        cache = null;
      }
      if (cache != null) {
        cache[nextFreeIndex] = element;
        nextFreeIndex++;
        size++;
      } else if (output != null) {
        for (long value : element) {
          output.writeLong(value);
        }
        size++;
      } else {
        throw new IllegalStateException("The " + IterableSortedLongArrayList.class.getName()
                + " is already closed or was already iterated.");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean isEmpty() {
    return nextFreeIndex == 0;
  }

  public long getSize() {
    return size;
  }

  public long getFreeCacheSpace() {
    return cache == null ? 0 : (cache.length - nextFreeIndex);
  }

  public LongIterator iterator() {
    if (!isSorted) {
      sort();
    }
    if (cache != null) {
      return new LongIterator() {

        private int nextUnreturnedArray = 0;

        private int nextUnreturnedElementWithinArray = 0;

        @Override
        public boolean hasNext() {
          return nextUnreturnedArray < nextFreeIndex;
        }

        @Override
        public long next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          long next = cache[nextUnreturnedArray][nextUnreturnedElementWithinArray++];
          if (nextUnreturnedElementWithinArray >= arraySize) {
            nextUnreturnedArray++;
            nextUnreturnedElementWithinArray = 0;
          }
          return next;
        }

        @Override
        public void close() {
        }
      };
    } else if (listFile.exists()) {
      return new LongIterator() {

        private EncodedLongFileInputStream input;

        private LongIterator iterator;

        {
          try {
            input = new EncodedLongFileInputStream(listFile);
            iterator = input.iterator();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public boolean hasNext() {
          return iterator.hasNext();
        }

        @Override
        public long next() {
          return iterator.next();
        }

        @Override
        public void close() {
          iterator.close();
          try {
            input.close();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      };
    } else {
      throw new IllegalStateException(
              "The " + IterableSortedLongArrayList.class.getName() + " is already closed.");
    }
  }

  private void sort() {
    if (cache != null) {
      Arrays.sort(cache, 0, nextFreeIndex, comparator);
    } else if (output != null) {
      InitialChunkProducer producer = null;
      Merger merger = null;
      try {
        output.close();
        output = null;
        producer = new InitialChunkProducer() {

          private long[][] elements = new long[(int) (cacheSize / arraySize
                  / Long.BYTES)][arraySize];

          private int nextFreeIndex = 0;

          private EncodedLongFileInputStream input;

          private LongIterator iterator = null;

          @Override
          public void loadNextChunk() throws IOException {
            if (iterator == null) {
              input = new EncodedLongFileInputStream(listFile);
              iterator = input.iterator();
            }
            nextFreeIndex = 0;
            while (iterator.hasNext() && (nextFreeIndex < elements.length)) {
              for (int i = 0; i < elements[nextFreeIndex].length; i++) {
                elements[nextFreeIndex][i] = iterator.next();
              }
              nextFreeIndex++;
            }
          }

          @Override
          public boolean hasNextChunk() {
            return nextFreeIndex > 0;
          }

          @Override
          public void sort(Comparator<long[]> comparator) {
            Arrays.sort(elements, 0, nextFreeIndex, comparator);
          }

          @Override
          public void writeChunk(LongOutputWriter output) throws IOException {
            int startIndex = 0;
            for (int exclusiveEndIndex = 1; exclusiveEndIndex <= nextFreeIndex; exclusiveEndIndex++) {
              if ((exclusiveEndIndex == nextFreeIndex) || (comparator.compare(elements[startIndex],
                      elements[exclusiveEndIndex]) != 0)) {
                for (long value : elements[startIndex]) {
                  output.writeLong(value);
                }
                startIndex = exclusiveEndIndex;
              }
            }
          }

          @Override
          public void close() {
            elements = null;
            try {
              input.close();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

        };

        merger = new Merger() {

          @Override
          public void startNextMergeLevel() {
          }

          @Override
          public long[] readNextElement(LongIterator iterator) throws IOException {
            long[] result = new long[arraySize];
            for (int i = 0; i < result.length; i++) {
              result[i] = iterator.next();
            }
            return result;
          }

          @Override
          public void mergeAndWrite(BitSet indicesOfSmallestElement, long[][] elements,
                  LongIterator[] iterators, LongOutputWriter out) throws IOException {
            int index = indicesOfSmallestElement.nextSetBit(0);
            for (long value : elements[index]) {
              out.writeLong(value);
            }
          }

          @Override
          public void close() {
          }
        };
        File newListFile = File.createTempFile("listFile", "", workingDir);
        NWayMergeSort sorter = new NWayMergeSort();
        sorter.sort(producer, merger, comparator, workingDir, maxNumberOfOpenFiles, newListFile);
        listFile.delete();
        listFile = newListFile;

      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        if (producer != null) {
          producer.close();
        }
        if (merger != null) {
          merger.close();
        }
      }
    } else {
      throw new IllegalStateException("The " + IterableSortedLongArrayList.class.getName()
              + " is already closed or was already iterated.");
    }
    isSorted = true;
  }

  @Override
  public void close() {
    if (output != null) {
      try {
        output.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      output = null;
    }
    if (listFile != null) {
      listFile.delete();
      listFile = null;
    }
  }

}
