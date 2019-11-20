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
package de.uni_koblenz.west.koral.master.graph_cover_creator.impl;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedFileOutputStream;
import de.uni_koblenz.west.koral.common.io.EncodedLongFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedLongFileOutputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.LongOutputWriter;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.measurement.MeasurementType;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.koral.master.dictionary.LongDictionary;
import de.uni_koblenz.west.koral.master.dictionary.impl.RocksDBDictionary;
import de.uni_koblenz.west.koral.master.utils.AdjacencyMatrix;
import de.uni_koblenz.west.koral.master.utils.DeSerializer;
import de.uni_koblenz.west.koral.master.utils.FixedSizeLongArrayComparator;
import de.uni_koblenz.west.koral.master.utils.InitialChunkProducer;
import de.uni_koblenz.west.koral.master.utils.LongIterator;
import de.uni_koblenz.west.koral.master.utils.Merger;
import de.uni_koblenz.west.koral.master.utils.NWayMergeSort;
import de.uni_koblenz.west.koral.master.utils.SingleFileAdjacencyMatrix;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Scanner;
import java.util.logging.Logger;

/**
 * Creates a minimal edge-cut cover with the help of
 * <a href="http://glaros.dtc.umn.edu/gkhome/metis/metis/overview">METIS</a>
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MinimalEdgeCutOverCover extends GraphCoverCreatorBase {

  private static final int MAX_NUMBER_OF_OPEN_FILES = 100;

  private static final long MAX_CASH_SIZE = 0x80_00_00L;

  private Process process;

  private long numberOfEdges;

  private long numberOfVertices;

  public MinimalEdgeCutOverCover(Logger logger, MeasurementCollector measurementCollector) {
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
    File dictionaryFolder = new File(
            workingDir.getAbsolutePath() + File.separator + "minEdgeCutDictionary");
    if (!dictionaryFolder.exists()) {
      dictionaryFolder.mkdirs();
    }
    LongDictionary localDictionary = new RocksDBDictionary(dictionaryFolder.getAbsolutePath(),
            RocksDBDictionary.DEFAULT_MAX_BATCH_SIZE, 50);

    File encodedRDFGraph = null;
    File metisOutputGraph = null;
    File ignoredTriples = new File(
            workingDir.getAbsolutePath() + File.separator + "ignoredTriples.gz");
    encodedRDFGraph = new File(
            workingDir.getAbsolutePath() + File.separator + "encodedRDFGraph.gz");
    File metisInputGraph = new File(workingDir.getAbsolutePath() + File.separator + "metisInput");

    createMetisInputFile(dictionary, input, localDictionary, encodedRDFGraph, metisInputGraph,
            ignoredTriples, workingDir);
    metisOutputGraph = runMetis(metisInputGraph,
            getNumberOfPartitions(187, numberOfEdges, numberOfVertices, numberOfGraphChunks));

    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_COVER_CREATION_FILE_WRITE_START,
              System.currentTimeMillis());
    }
    File vertex2chunkIndexFolder = new File(
            workingDir.getAbsolutePath() + File.separator + "vertex2chunkIndex");
    if (!vertex2chunkIndexFolder.exists()) {
      vertex2chunkIndexFolder.mkdirs();
    }
    RocksDB vertex2chunkIndex = null;
    try {
      vertex2chunkIndex = convertMetisOutputToVertex2PartitionMap(metisOutputGraph,
              vertex2chunkIndexFolder);
      createGraphCover(encodedRDFGraph, localDictionary, vertex2chunkIndex, ignoredTriples, outputs,
              writtenFiles, numberOfGraphChunks, workingDir);

    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      if (vertex2chunkIndex != null) {
        vertex2chunkIndex.close();
      }
    }
    deleteFolder(vertex2chunkIndexFolder);
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_COVER_CREATION_FILE_WRITE_END,
              System.currentTimeMillis());
    }

    // tidy up
    metisInputGraph.delete();
    if (encodedRDFGraph != null) {
      encodedRDFGraph.delete();
    }
    if (metisOutputGraph != null) {
      metisOutputGraph.delete();
    }
    if (ignoredTriples.exists()) {
      ignoredTriples.delete();
    }
    localDictionary.close();
    deleteFolder(dictionaryFolder);
  }

  private void createMetisInputFile(DictionaryEncoder dictionary, EncodedFileInputStream input,
          LongDictionary localDictionary, File encodedRDFGraph, File metisInputGraph,
          File ignoredTriples, File workingDir) {
    if (measurementCollector != null) {
      measurementCollector.measureValue(
              MeasurementType.LOAD_GRAPH_COVER_CREATION_METIS_INPUT_FILE_CREATION_START,
              System.currentTimeMillis());
    }
    File metisInputTempFolder = new File(
            workingDir.getAbsolutePath() + File.separator + "metisInputCreation");
    if (!metisInputTempFolder.exists()) {
      metisInputTempFolder.mkdirs();
    }

    long encodedRdfTypeLabel = dictionary.encodeWithoutOwnership(
            DeSerializer.deserializeNode("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"),
            false);

    long numberOfVertices = 0;
    long numberOfEdges = 0;
    long numberOfUsedTriples = 0;
    long numberOfIgnoredTriples = 0;

    AdjacencyMatrix adjacencyMatrix = new SingleFileAdjacencyMatrix(metisInputTempFolder);
    // create adjacency lists
    try {
      try (EncodedFileOutputStream encodedGraphOutput = new EncodedFileOutputStream(
              encodedRDFGraph);
              EncodedFileOutputStream ignoredTriplesOutput = new EncodedFileOutputStream(
                      ignoredTriples);) {
        for (Statement statement : input) {
          if (Arrays.equals(statement.getSubject(), statement.getObject())
                  || (statement.getPropertyAsLong() == encodedRdfTypeLabel)) {
            // this is a self loop that is forbidden in METIS or
            // it is a rdf:type triple
            // store it as ignored triple
            numberOfIgnoredTriples++;
            ignoredTriplesOutput.writeStatement(statement);
            continue;
          }
          numberOfUsedTriples++;
          long encodedSubject = localDictionary.encode(statement.getSubjectAsLong(), true);
          if (encodedSubject > numberOfVertices) {
            numberOfVertices = encodedSubject;
          }
          long encodedObject = localDictionary.encode(statement.getObjectAsLong(), true);
          if (encodedObject > numberOfVertices) {
            numberOfVertices = encodedObject;
          }
          // write encoded triple to graph file
          Statement encodedStatement = Statement.getStatement(getRequiredInputEncoding(),
                  NumberConversion.long2bytes(encodedSubject), statement.getProperty(),
                  NumberConversion.long2bytes(encodedObject), statement.getContainment());
          encodedGraphOutput.writeStatement(encodedStatement);

          adjacencyMatrix.addEdge(encodedSubject, encodedObject);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      numberOfVertices = adjacencyMatrix.getNumberOfVertices();
      numberOfEdges = adjacencyMatrix.getNumberOfEdges();

      if (measurementCollector != null) {
        measurementCollector.measureValue(
                MeasurementType.LOAD_GRAPH_COVER_CREATION_METIS_IGNORED_TRIPLES,
                numberOfIgnoredTriples);
        measurementCollector.measureValue(
                MeasurementType.LOAD_GRAPH_COVER_CREATION_METIS_INPUT_GRAPH_SIZE,
                Long.toString(numberOfUsedTriples), Long.toString(numberOfVertices),
                Long.toString(numberOfEdges));
      }

      // write adjacency lists to file
      try (BufferedWriter metisInputGraphWriter = new BufferedWriter(
              new OutputStreamWriter(new FileOutputStream(metisInputGraph), "UTF-8"));) {
        metisInputGraphWriter.write(numberOfVertices + " " + numberOfEdges);
        for (long vertex = 1; vertex <= numberOfVertices; vertex++) {
          metisInputGraphWriter.write("\n");
          String delim = "";
          LongIterator iterator = adjacencyMatrix.getAdjacencyList(vertex);
          while (iterator.hasNext()) {
            long neighbour = iterator.next();
            metisInputGraphWriter.write(delim + neighbour);
            delim = " ";
          }
          iterator.close();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

    } finally {
      localDictionary.flush();
      adjacencyMatrix.close();
      if (measurementCollector != null) {
        measurementCollector.measureValue(
                MeasurementType.LOAD_GRAPH_COVER_CREATION_METIS_INPUT_FILE_CREATION_END,
                System.currentTimeMillis());
      }
    }
    deleteFolder(metisInputTempFolder);
    this.numberOfVertices = numberOfVertices;
    this.numberOfEdges = numberOfEdges;
  }

  protected int getNumberOfPartitions(int lambda, long numberOfEdges, long numberOfVertices,
          int numberOfGraphChunks) {
    return (int) Math.ceil(Math.sqrt((((double) lambda) * numberOfVertices) / numberOfGraphChunks));
  }

  private File runMetis(File metisInputGraph, int numberOfGraphChunks) {
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_COVER_CREATION_RUN_METIS_START,
              System.currentTimeMillis());
    }
    ProcessBuilder processBuilder = new ProcessBuilder("gpmetis", metisInputGraph.getAbsolutePath(),
            Integer.valueOf(numberOfGraphChunks).toString());
    if (logger == null) {
      processBuilder.inheritIO();
    }
    try {
      process = processBuilder.start();
      if (logger != null) {
        new LogPipedWriter(process.getInputStream(), logger).start();
        new LogPipedWriter(process.getErrorStream(), logger).start();
      }
      process.waitFor();
      process = null;
      return new File(metisInputGraph.getAbsolutePath() + ".part." + numberOfGraphChunks);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      if ((process != null) && process.isAlive()) {
        process.destroy();
      }
      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_COVER_CREATION_RUN_METIS_END,
                System.currentTimeMillis());
      }
    }
  }

  protected void createGraphCover(File encodedRDFGraph, LongDictionary dictionary,
          RocksDB vertex2chunkIndex, File ignoredTriples, EncodedFileOutputStream[] outputs,
          boolean[] writtenFiles, int numberOfGraphChunks, File workingDir) {
    int bitsetSize = numberOfGraphChunks / Byte.SIZE;
    if ((numberOfGraphChunks % Byte.SIZE) != 0) {
      bitsetSize += 1;
    }
    byte[] containment = new byte[bitsetSize];

    // convert input to a file with each row
    // partitionID, #triples, tripel1, tripel2, ....
    File partitions = createPartitions(encodedRDFGraph, dictionary, vertex2chunkIndex,
            ignoredTriples, workingDir, MinimalEdgeCutOverCover.MAX_NUMBER_OF_OPEN_FILES,
            MinimalEdgeCutOverCover.MAX_CASH_SIZE);

    // sort partitions by their size in descending order
    // (partitionID, #triples)*
    File sortedPartitions = sortPartitionsBySizeDescending(partitions, workingDir,
            MinimalEdgeCutOverCover.MAX_NUMBER_OF_OPEN_FILES,
            MinimalEdgeCutOverCover.MAX_CASH_SIZE);

    // assign partitions to chunks
    // (partitionID, chunkID)*
    File partition2chunk = assignPartitions2Chunks(sortedPartitions, numberOfGraphChunks,
            workingDir);
    sortedPartitions.delete();

    // sort partitions by their IDs
    // (partitionID, chunkID)*
    File partition2chunkSorted = sortPartitionsByID(partition2chunk, workingDir,
            MinimalEdgeCutOverCover.MAX_NUMBER_OF_OPEN_FILES,
            MinimalEdgeCutOverCover.MAX_CASH_SIZE);
    partition2chunk.delete();

    try (EncodedLongFileInputStream partitionsInput = new EncodedLongFileInputStream(partitions);
            LongIterator partitionsIterator = partitionsInput.iterator();
            EncodedLongFileInputStream partition2chunkSortedInput = new EncodedLongFileInputStream(
                    partition2chunkSorted);
            LongIterator partition2chunkSortedIterator = partition2chunkSortedInput.iterator();) {
      while (partitionsIterator.hasNext()) {
        long partition = partitionsIterator.next();
        long numberOfTriples = partitionsIterator.next();
        long checkPartition = partition2chunkSortedIterator.next();
        if (partition != checkPartition) {
          throw new RuntimeException(partition > checkPartition
                  ? "The partition " + checkPartition + " does not contain any triples."
                  : "The partition " + partition + " was not assigned to a chunk.");
        }
        int chunkId = (int) partition2chunkSortedIterator.next();
        for (long i = 0; i < numberOfTriples; i++) {
          long subject = partitionsIterator.next();
          long property = partitionsIterator.next();
          long object = partitionsIterator.next();
          Statement statement = Statement.getStatement(getRequiredInputEncoding(),
                  NumberConversion.long2bytes(subject), NumberConversion.long2bytes(property),
                  NumberConversion.long2bytes(object), containment);
          writeStatementToChunk(chunkId, numberOfGraphChunks, statement, outputs, writtenFiles);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    partition2chunkSorted.delete();
    partitions.delete();
  }

  /**
   * @param partition2chunk
   * @param workingDir
   * @param maxNumberOfOpenFiles
   * @param maxCashSize
   * @return (partitionID, chunkID)*
   */
  private File sortPartitionsByID(File partition2chunk, File workingDir, int maxNumberOfOpenFiles,
          long maxCashSize) {
    InitialChunkProducer producer = null;
    Merger merger = null;
    try {
      producer = new InitialChunkProducer() {

        /**
         * partitionID, #triples, (tripel)^{#triples}
         */
        private long[][] elements;

        private int nextIndex;

        private EncodedLongFileInputStream input;

        private LongIterator iterator;

        @Override
        public void loadNextChunk() throws IOException {
          if (elements == null) {
            elements = new long[(int) (maxCashSize / Long.BYTES / 2)][2];
            input = new EncodedLongFileInputStream(partition2chunk);
            iterator = input.iterator();
          }
          for (nextIndex = 0; (nextIndex < elements.length) && iterator.hasNext(); nextIndex++) {
            elements[nextIndex][0] = iterator.next();
            elements[nextIndex][1] = iterator.next();
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
          for (int element = 0; element < nextIndex; element++) {
            for (long value : elements[element]) {
              output.writeLong(value);
            }
          }
        }

        @Override
        public void close() {
          if (iterator != null) {
            iterator.close();
          }
          if (input != null) {
            try {
              input.close();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      };
      merger = new Merger() {

        @Override
        public void startNextMergeLevel() {
        }

        @Override
        public long[] readNextElement(LongIterator iterator) throws IOException {
          return new long[] { iterator.next(), iterator.next() };
        }

        @Override
        public void mergeAndWrite(BitSet indicesOfSmallestElement, long[][] elements,
                LongIterator[] iterators, LongOutputWriter out) throws IOException {
          for (int i = indicesOfSmallestElement.nextSetBit(0); i >= 0; i = indicesOfSmallestElement
                  .nextSetBit(i + 1)) {
            for (long value : elements[i]) {
              out.writeLong(value);
            }
          }
        }

        @Override
        public void close() {
        }
      };
      Comparator<long[]> comparator = new FixedSizeLongArrayComparator(true, 0);
      File partition2chunkSorted = File.createTempFile("partition2chunkSorted", "", workingDir);
      NWayMergeSort sort = new NWayMergeSort();
      sort.sort(producer, merger, comparator, workingDir, maxNumberOfOpenFiles,
              partition2chunkSorted);
      return partition2chunkSorted;
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

  /**
   * @param sortedPartitions
   * @param numberOfGraphChunks
   * @param workingDir
   * @return (partitionID, chunkID)*
   */
  private File assignPartitions2Chunks(File sortedPartitions, int numberOfGraphChunks,
          File workingDir) {
    try (EncodedLongFileInputStream input = new EncodedLongFileInputStream(sortedPartitions);
            LongIterator iterator = input.iterator();) {
      File partition2chunk = File.createTempFile("partition2chunk", "", workingDir);
      try (EncodedLongFileOutputStream output = new EncodedLongFileOutputStream(partition2chunk);) {
        long[] chunkSizes = new long[numberOfGraphChunks];
        while (iterator.hasNext()) {
          int indexOfSmallestChunk = 0;
          for (int i = 0; i < chunkSizes.length; i++) {
            if (chunkSizes[i] < chunkSizes[indexOfSmallestChunk]) {
              indexOfSmallestChunk = i;
            }
          }
          long partitionId = iterator.next();
          long paritionSize = iterator.next();
          chunkSizes[indexOfSmallestChunk] += paritionSize;
          output.writeLong(partitionId);
          output.writeLong(indexOfSmallestChunk);
        }
      }
      return partition2chunk;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @param partitions
   * @param workingDir
   * @param maxNumberOfOpenFiles
   * @param maxCashSize
   * @return (partitionID, #triples)*
   */
  private File sortPartitionsBySizeDescending(File partitions, File workingDir,
          int maxNumberOfOpenFiles, long maxCashSize) {
    InitialChunkProducer producer = null;
    Merger merger = null;
    try {
      producer = new InitialChunkProducer() {

        /**
         * partitionID, #triples, (tripel)^{#triples}
         */
        private long[][] elements;

        private int nextIndex;

        private EncodedLongFileInputStream input;

        private LongIterator iterator;

        @Override
        public void loadNextChunk() throws IOException {
          if (elements == null) {
            elements = new long[(int) (maxCashSize / Long.BYTES / 2)][2];
            input = new EncodedLongFileInputStream(partitions);
            iterator = input.iterator();
          }
          for (nextIndex = 0; (nextIndex < elements.length) && iterator.hasNext(); nextIndex++) {
            elements[nextIndex][0] = iterator.next();
            elements[nextIndex][1] = iterator.next();
            for (int i = 0; i < elements[nextIndex][1]; i++) {
              iterator.next();
              iterator.next();
              iterator.next();
            }
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
          for (int element = 0; element < nextIndex; element++) {
            for (long value : elements[element]) {
              output.writeLong(value);
            }
          }
        }

        @Override
        public void close() {
          if (iterator != null) {
            iterator.close();
          }
          if (input != null) {
            try {
              input.close();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      };
      merger = new Merger() {

        @Override
        public void startNextMergeLevel() {
        }

        @Override
        public long[] readNextElement(LongIterator iterator) throws IOException {
          return new long[] { iterator.next(), iterator.next() };
        }

        @Override
        public void mergeAndWrite(BitSet indicesOfSmallestElement, long[][] elements,
                LongIterator[] iterators, LongOutputWriter out) throws IOException {
          for (int i = indicesOfSmallestElement.nextSetBit(0); i >= 0; i = indicesOfSmallestElement
                  .nextSetBit(i + 1)) {
            for (long value : elements[i]) {
              out.writeLong(value);
            }
          }
        }

        @Override
        public void close() {
        }
      };
      Comparator<long[]> comparator = new FixedSizeLongArrayComparator(false, 1, 0);
      File sortedPartitions = File.createTempFile("sortedPartitions", "", workingDir);
      NWayMergeSort sort = new NWayMergeSort();
      sort.sort(producer, merger, comparator, workingDir, maxNumberOfOpenFiles, sortedPartitions);
      return sortedPartitions;
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

  /**
   * @param encodedRDFGraph
   * @param dictionary
   * @param vertex2chunkIndex
   * @param ignoredTriples
   * @param workingDir
   * @param maxNumberOfOpenFiles
   * @param maxCashSize
   * @return (partitionID, #triples, (tripel)^{#triples})*
   */
  private File createPartitions(File encodedRDFGraph, LongDictionary dictionary,
          RocksDB vertex2chunkIndex, File ignoredTriples, File workingDir, int maxNumberOfOpenFiles,
          long maxCashSize) {
    InitialChunkProducer producer = null;
    Merger merger = null;
    try {
      producer = new InitialChunkProducer() {

        /**
         * partitionID, #triples, (tripel)^{#triples}
         */
        private long[][] elements;

        private int nextIndex;

        private boolean isIgnoredTriples = false;

        private EncodedFileInputStream graphInput = new EncodedFileInputStream(
                getRequiredInputEncoding(), encodedRDFGraph);

        private Iterator<Statement> iterator;

        private long lastVertex = -1;

        private long lastChunkIndex = -1;

        private long highestChunkNumber = Long.MIN_VALUE;

        @Override
        public void loadNextChunk() throws IOException {
          if (graphInput == null) {
            nextIndex = 0;
            return;
          }
          if (elements == null) {
            elements = new long[(int) (maxCashSize / Long.BYTES / 5)][5];
          }
          if (iterator == null) {
            iterator = graphInput.iterator();
          }
          try {
            for (nextIndex = 0; nextIndex < elements.length; nextIndex++) {
              if (iterator.hasNext()) {
                Statement statement = iterator.next();
                long subject;
                long property = statement.getPropertyAsLong();
                long object;
                if (!isIgnoredTriples) {
                  subject = dictionary.decodeLong(statement.getSubjectAsLong());
                  object = dictionary.decodeLong(statement.getObjectAsLong());
                } else {
                  subject = statement.getSubjectAsLong();
                  object = statement.getObjectAsLong();
                }
                long targetChunk = -1;
                if (subject == lastVertex) {
                  targetChunk = lastChunkIndex;
                } else {
                  byte[] targetBytes = vertex2chunkIndex.get(NumberConversion.long2bytes(subject));
                  if (targetBytes == null) {
                    // if a vertex only occurs in a self loop or with a rdf:type
                    // property it is not partitioned by
                    // metis
                    highestChunkNumber++;
                    targetChunk = highestChunkNumber;
                    vertex2chunkIndex.put(NumberConversion.long2bytes(subject),
                            NumberConversion.int2bytes((int) targetChunk));
                  } else {
                    targetChunk = NumberConversion.bytes2int(targetBytes);
                    lastVertex = subject;
                    lastChunkIndex = targetChunk;
                    if (targetChunk > highestChunkNumber) {
                      highestChunkNumber = targetChunk;
                    }
                  }
                }
                elements[nextIndex][0] = targetChunk;
                elements[nextIndex][1] = 1;
                elements[nextIndex][2] = subject;
                elements[nextIndex][3] = property;
                elements[nextIndex][4] = object;
              } else {
                if (graphInput != null) {
                  graphInput.close();
                }
                if (!isIgnoredTriples) {
                  graphInput = new EncodedFileInputStream(getRequiredInputEncoding(),
                          ignoredTriples);
                  isIgnoredTriples = true;
                  iterator = graphInput.iterator();
                  nextIndex--;
                } else {
                  graphInput = null;
                  iterator = null;
                  break;
                }
              }
            }
          } catch (RocksDBException e) {
            throw new RuntimeException(e);
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
          for (int nextIndexToWrite = 0; nextIndexToWrite < nextIndex; nextIndexToWrite++) {
            long currentChunk = elements[nextIndexToWrite][0];
            int lastIndexToWrite = nextIndexToWrite + 1;
            while ((lastIndexToWrite < elements.length)
                    && (elements[lastIndexToWrite][0] == currentChunk)) {
              lastIndexToWrite++;
            }
            long numberOfTriplesInChunk = lastIndexToWrite - nextIndexToWrite;
            output.writeLong(currentChunk);
            output.writeLong(numberOfTriplesInChunk);
            while (nextIndexToWrite < lastIndexToWrite) {
              output.writeLong(elements[nextIndexToWrite][2]);
              output.writeLong(elements[nextIndexToWrite][3]);
              output.writeLong(elements[nextIndexToWrite][4]);
              nextIndexToWrite++;
            }
            nextIndexToWrite--;
          }
        }

        @Override
        public void close() {
          if (graphInput != null) {
            try {
              graphInput.close();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      };
      merger = new Merger() {

        @Override
        public void startNextMergeLevel() {
        }

        @Override
        public long[] readNextElement(LongIterator iterator) throws IOException {
          return new long[] { iterator.next(), iterator.next() };
        }

        @Override
        public void mergeAndWrite(BitSet indicesOfSmallestElement, long[][] elements,
                LongIterator[] iterators, LongOutputWriter out) throws IOException {
          long chunkId = elements[indicesOfSmallestElement.nextSetBit(0)][0];
          long chunkSize = 0;
          for (int i = indicesOfSmallestElement.nextSetBit(0); i >= 0; i = indicesOfSmallestElement
                  .nextSetBit(i + 1)) {
            chunkSize += elements[i][1];
          }
          out.writeLong(chunkId);
          out.writeLong(chunkSize);
          for (int i = indicesOfSmallestElement.nextSetBit(0); i >= 0; i = indicesOfSmallestElement
                  .nextSetBit(i + 1)) {
            long numberOfTriples = elements[i][1];
            for (long tripleNr = 0; tripleNr < numberOfTriples; tripleNr++) {
              out.writeLong(iterators[i].next());
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
      File triplesPerPartition = File.createTempFile("triplesPerPartition", "", workingDir);
      NWayMergeSort sort = new NWayMergeSort();
      sort.sort(producer, merger, comparator, workingDir, maxNumberOfOpenFiles,
              triplesPerPartition);
      return triplesPerPartition;
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

  private RocksDB convertMetisOutputToVertex2PartitionMap(File metisOutputGraph,
          File vertex2chunkIndexFolder) throws RocksDBException {
    RocksDB vertex2chunkIndex = null;
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setMaxOpenFiles(800);
    options.setWriteBufferSize(64 * 1024 * 1024);
    // load mapping vertex -> chunkIndex from metis output
    vertex2chunkIndex = RocksDB.open(options,
            vertex2chunkIndexFolder.getAbsolutePath() + File.separator + "vertex2chunkIndex");
    try (Scanner scanner = new Scanner(metisOutputGraph);) {
      scanner.useDelimiter("\\r?\\n");
      long vertex = 1;
      while (scanner.hasNextInt()) {
        int chunkIndex = scanner.nextInt();
        vertex2chunkIndex.put(NumberConversion.long2bytes(vertex),
                NumberConversion.int2bytes(chunkIndex));
        vertex++;
      }
    } catch (FileNotFoundException | RocksDBException e) {
      throw new RuntimeException(e);
    }
    return vertex2chunkIndex;
  }

  private void deleteFolder(File folder) {
    if (!folder.exists()) {
      return;
    }
    if (folder.isDirectory()) {
      Path path = FileSystems.getDefault().getPath(folder.getAbsolutePath());
      try {
        Files.walkFileTree(path, new FileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                  throws IOException {
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                  throws IOException {
            // here you have the files to process
            file.toFile().delete();
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            return FileVisitResult.TERMINATE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
          }
        });
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    folder.delete();
  }

  @Override
  public void close() {
    if ((process != null) && process.isAlive()) {
      process.destroy();
    }
    super.close();
  }

}
