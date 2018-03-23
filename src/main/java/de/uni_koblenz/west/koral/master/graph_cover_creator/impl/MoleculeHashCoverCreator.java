package de.uni_koblenz.west.koral.master.graph_cover_creator.impl;

import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedFileOutputStream;
import de.uni_koblenz.west.koral.common.io.EncodedLongFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.LongOutputWriter;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.utils.Deleter;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.koral.master.utils.FixedSizeLongArrayComparator;
import de.uni_koblenz.west.koral.master.utils.InitialChunkProducer;
import de.uni_koblenz.west.koral.master.utils.LongIterator;
import de.uni_koblenz.west.koral.master.utils.Merger;
import de.uni_koblenz.west.koral.master.utils.NWayMergeSort;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.logging.Logger;

/**
 * Creates a molecule hash cover.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MoleculeHashCoverCreator extends GraphCoverCreatorBase {

  private static final int MAX_NUMBER_OF_OPEN_FILES = 100;

  private static final long MAX_CASH_SIZE = 0x80_00_00L;

  public MoleculeHashCoverCreator(Logger logger, MeasurementCollector measurementCollector) {
    super(logger, measurementCollector);
  }

  @Override
  public EncodingFileFormat getRequiredInputEncoding() {
    return EncodingFileFormat.EEE;
  }

  @Override
  protected void createCover(DictionaryEncoder dictionary, EncodedFileInputStream input,
          int numberOfGraphChunks, EncodedFileOutputStream[] outputs, boolean[] writtenFiles,
          File workingDir) {
    File internalWorkingDir = new File(
            workingDir + File.separator + this.getClass().getSimpleName());
    if (!internalWorkingDir.exists()) {
      internalWorkingDir.mkdirs();
    }

    File adjacencyOutListsSortedByInDegree = createAdjacencyListsSortedByInDegree(input,
            internalWorkingDir, MoleculeHashCoverCreator.MAX_NUMBER_OF_OPEN_FILES,
            MoleculeHashCoverCreator.MAX_CASH_SIZE);

    Deleter.deleteFolder(internalWorkingDir);
  }

  /**
   * @param input
   * @param internalWorkingDir
   * @param maxNumberOfOpenFiles
   * @param maxCashSize
   * @return (startVertexID, inDegree, outDegree, (outEdge, endVertexId)*)*
   *         sorted by inDegree
   */
  private File createAdjacencyListsSortedByInDegree(EncodedFileInputStream input, File workingDir,
          int maxNumberOfOpenFiles, long maxCashSize) {
    File adjacencyListSortedByVertex = createAdjacencyListsSortedByStartVertexId(input, workingDir,
            maxNumberOfOpenFiles, maxCashSize);
    // TODO remove
    print(adjacencyListSortedByVertex);
    // TODO Auto-generated method stub
    return null;
  }

  private void print(File adjacencyList) {
    try (EncodedLongFileInputStream input = new EncodedLongFileInputStream(adjacencyList);) {
      LongIterator iterator = input.iterator();
      while (iterator.hasNext()) {
        StringBuilder sb = new StringBuilder();
        long startVertexId = iterator.next();
        long indegree = iterator.next();
        long outdegree = iterator.next();
        sb.append("v" + startVertexId + " indegree:" + indegree + " outdegree:" + outdegree + "\n");
        for (int i = 0; i < outdegree; i++) {
          long edgeId = iterator.next();
          long endVertexId = iterator.next();
          sb.append("\te" + edgeId + "->v" + endVertexId);
        }
        System.out.println(sb.toString());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @param input
   * @param workingDir
   * @param maxNumberOfOpenFiles
   * @param maxCashSize
   * @return (startVertexID, inDegree, outDegree, (outEdge, endVertexId)*)*
   *         sorted by startVertexID
   */
  private File createAdjacencyListsSortedByStartVertexId(EncodedFileInputStream input,
          File workingDir, int maxNumberOfOpenFiles, long maxCashSize) {
    InitialChunkProducer producer = null;
    Merger merger = null;
    try {
      producer = new InitialChunkProducer() {

        /**
         * format {startVertexId, indegree, outdegree, (edgeId, endVertexId)?}
         */
        private long[][] elements;

        private int nextIndex;

        private final Iterator<Statement> iterator = input.iterator();

        private long nextEdgeId;

        @Override
        public void loadNextChunk() throws IOException {
          if (elements == null) {
            long numberOfElements = maxCashSize / Long.BYTES / 5;
            elements = new long[(int) numberOfElements][5];
            nextEdgeId = 1;
          }
          nextIndex = 0;
          while (iterator.hasNext() && (nextIndex < (elements.length - 1))) {
            Statement stmt = iterator.next();
            elements[nextIndex][0] = stmt.getSubjectAsLong();
            elements[nextIndex][1] = 0;
            elements[nextIndex][2] = 1;
            elements[nextIndex][3] = nextEdgeId;
            elements[nextIndex][4] = stmt.getObjectAsLong();
            nextIndex += 1;
            nextEdgeId++;

            elements[nextIndex][0] = stmt.getObjectAsLong();
            elements[nextIndex][1] = 1;
            elements[nextIndex][2] = 0;
            elements[nextIndex][3] = 0;
            elements[nextIndex][4] = 0;
            nextIndex += 1;
          }
        }

        @Override
        public void sort(Comparator<long[]> comparator) {
          Arrays.sort(elements, 0, nextIndex, comparator);
        }

        @Override
        public boolean hasNextChunk() {
          return nextIndex > 0;
        }

        @Override
        public void writeChunk(LongOutputWriter output) throws IOException {
          int startIndex = 0;
          for (int exclusiveEndIndex = 1; exclusiveEndIndex <= nextIndex; exclusiveEndIndex++) {
            if ((exclusiveEndIndex == elements.length)
                    || (elements[startIndex][0] != elements[exclusiveEndIndex][0])) {
              output.writeLong(elements[startIndex][0]);
              long indegree = 0;
              long outdegree = 0;
              for (int i = startIndex; i < exclusiveEndIndex; i++) {
                indegree += elements[i][1];
                outdegree += elements[i][2];
              }
              output.writeLong(indegree);
              output.writeLong(outdegree);
              for (int i = startIndex; i < exclusiveEndIndex; i++) {
                if (elements[i][2] > 0) {
                  output.writeLong(elements[i][3]);
                  output.writeLong(elements[i][4]);
                }
              }
              startIndex = exclusiveEndIndex;
            }
          }
        }

        @Override
        public void close() {
          elements = null;
        }
      };
      merger = new Merger() {

        @Override
        public void startNextMergeLevel() {
        }

        @Override
        public long[] readNextElement(LongIterator iterator) throws IOException {
          return new long[] { iterator.next(), iterator.next(), iterator.next() };
        }

        @Override
        public void mergeAndWrite(BitSet indicesOfSmallestElement, long[][] elements,
                LongIterator[] iterators, LongOutputWriter out) throws IOException {
          out.writeLong(elements[indicesOfSmallestElement.nextSetBit(0)][0]);
          long indegree = 0;
          long outdegree = 0;
          for (int i = indicesOfSmallestElement.nextSetBit(0); i >= 0; i = indicesOfSmallestElement
                  .nextSetBit(i + 1)) {
            indegree += elements[i][1];
            outdegree += elements[i][2];
          }
          out.writeLong(indegree);
          out.writeLong(outdegree);
          for (int i = indicesOfSmallestElement.nextSetBit(0); i >= 0; i = indicesOfSmallestElement
                  .nextSetBit(i + 1)) {
            for (int j = 0; j < elements[i][2]; j++) {
              out.writeLong(iterators[i].next());
              out.writeLong(iterators[i].next());
            }
          }
        }

        @Override
        public void close() {
        }
      };
      Comparator<long[]> comparator = new FixedSizeLongArrayComparator(true, 0);
      File adjacencyListSortedByVertex = File.createTempFile("adjacencyListSortedByVertex", "",
              workingDir);
      NWayMergeSort sort = new NWayMergeSort();
      sort.sort(producer, merger, comparator, workingDir, maxNumberOfOpenFiles,
              adjacencyListSortedByVertex);
      return adjacencyListSortedByVertex;
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
  }

  @Override
  public void close() {
    super.close();
  }

}
