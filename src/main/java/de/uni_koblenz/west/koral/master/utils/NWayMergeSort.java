package de.uni_koblenz.west.koral.master.utils;

import de.uni_koblenz.west.koral.common.io.EncodedLongFileInputIterator;
import de.uni_koblenz.west.koral.common.io.EncodedLongFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedLongFileOutputStream;
import de.uni_koblenz.west.koral.common.io.LongOutputWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;

/**
 * 
 * Performs an n-way merge sort.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class NWayMergeSort {

  public void sort(InitialChunkProducer producer, Merger merger, Comparator<long[]> comparator,
          File workingDir, int maxNumberOfOpenFiles, LongOutputWriter output) {
    maxNumberOfOpenFiles -= 1;
    try {
      List<File> chunks = new ArrayList<>();
      // create initial chunks
      producer.loadNextChunk();
      while (producer.hasNextChunk()) {
        File chunk = File.createTempFile("initialChunk-", "", workingDir);
        chunks.add(chunk);
        try (EncodedLongFileOutputStream chunkOut = new EncodedLongFileOutputStream(chunk);) {
          producer.sort(comparator);
          producer.writeChunk(chunkOut);
        }
        producer.loadNextChunk();
      }
      producer.close();
      // merge
      while (!chunks.isEmpty()) {
        List<File> mergedChunks = new ArrayList<>();
        for (int iterationStart = 0; iterationStart < chunks
                .size(); iterationStart += maxNumberOfOpenFiles - 1) {
          int numberOfProcessedFiles = Math.min(maxNumberOfOpenFiles - 1,
                  chunks.size() - iterationStart);
          EncodedLongFileInputStream[] inputs = new EncodedLongFileInputStream[numberOfProcessedFiles];
          EncodedLongFileInputIterator[] iterators = new EncodedLongFileInputIterator[numberOfProcessedFiles];
          LongOutputWriter out = null;
          try {
            long[][] nextElements = new long[numberOfProcessedFiles][];
            // initialize merge step
            for (int i = 0; i < numberOfProcessedFiles; i++) {
              inputs[i] = new EncodedLongFileInputStream(chunks.get(iterationStart + i));
              iterators[i] = new EncodedLongFileInputIterator(inputs[i]);
              if (iterators[i].hasNext()) {
                nextElements[i] = merger.readNextElement(iterators[i]);
              } else {
                nextElements[i] = null;
                iterators[i].close();
                iterators[i] = null;
              }
            }
            if (chunks.size() <= (maxNumberOfOpenFiles - 1)) {
              out = output;
            } else {
              File chunk = File.createTempFile("mergeChunk-", "", workingDir);
              out = new EncodedLongFileOutputStream(chunk);
            }
            // perform merge step
            for (BitSet indicesOfSmallestElement = getIndicesOfSmallestElement(nextElements,
                    comparator); !indicesOfSmallestElement
                            .isEmpty(); indicesOfSmallestElement = getIndicesOfSmallestElement(
                                    nextElements, comparator)) {
              merger.mergeAndWrite(indicesOfSmallestElement, nextElements, iterators, out);
              // update next elements
              for (int i = indicesOfSmallestElement
                      .nextSetBit(0); i >= 0; i = indicesOfSmallestElement.nextSetBit(i + 1)) {
                if (iterators[i].hasNext()) {
                  nextElements[i] = merger.readNextElement(iterators[i]);
                } else {
                  nextElements[i] = null;
                  iterators[i].close();
                  iterators[i] = null;
                }
              }
            }

          } finally {
            if (out != null) {
              out.close();
            }
            for (EncodedLongFileInputIterator iter : iterators) {
              if (iter != null) {
                iter.close();
              }
            }
            for (EncodedLongFileInputStream input : inputs) {
              if (input != null) {
                input.close();
              }
            }
          }
          for (File chunk : chunks) {
            chunk.delete();
          }
        }
        chunks = mergedChunks;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private BitSet getIndicesOfSmallestElement(long[][] elements, Comparator<long[]> comparator) {
    long[] smallestElement = null;
    BitSet smallestIndices = new BitSet(elements.length);
    for (int i = 0; i < elements.length; i++) {
      if (elements[i] == null) {
        continue;
      }
      if ((smallestElement == null) || (comparator.compare(elements[i], smallestElement) < 0)) {
        smallestElement = elements[i];
        smallestIndices.clear();
        smallestIndices.set(i);
      } else if (comparator.compare(elements[i], smallestElement) == 0) {
        smallestIndices.set(i);
      }
    }
    return smallestIndices;
  }

}
