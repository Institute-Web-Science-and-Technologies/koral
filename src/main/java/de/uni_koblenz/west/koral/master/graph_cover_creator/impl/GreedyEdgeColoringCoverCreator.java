package de.uni_koblenz.west.koral.master.graph_cover_creator.impl;

import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedFileOutputStream;
import de.uni_koblenz.west.koral.common.io.EncodedLongFileInputIterator;
import de.uni_koblenz.west.koral.common.io.EncodedLongFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedLongFileOutputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.koral.master.utils.LongIterator;
import de.uni_koblenz.west.koral.master.utils.VertexIncidencentEdgesListFileCreator;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Logger;

/**
 * Creates a greedy edge coloring cover.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class GreedyEdgeColoringCoverCreator extends GraphCoverCreatorBase {

  // TODO adjust
  private static final int NUMBER_OF_CACHED_VERTICES = 0x20;// 0x10_00_00;

  private static final int MAX_NUMBER_OF_OPEN_FILES = 100;

  public GreedyEdgeColoringCoverCreator(Logger logger, MeasurementCollector measurementCollector) {
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
    // TODO remove
    long start = System.currentTimeMillis();
    File internalWorkingDir = new File(workingDir + File.separator + "edgeColoringCoverCreator");
    if (!internalWorkingDir.exists()) {
      internalWorkingDir.mkdirs();
    }

    // transform into vertex,outDegree,inDegree,outEdgeList,inEdgeList format
    List<File> initialChunks = createInitialChunks(input, internalWorkingDir,
            GreedyEdgeColoringCoverCreator.NUMBER_OF_CACHED_VERTICES);
    File vertexIncidentEdgesFile = sortAndMerge(initialChunks, internalWorkingDir,
            GreedyEdgeColoringCoverCreator.MAX_NUMBER_OF_OPEN_FILES);

    print(vertexIncidentEdgesFile);

    // TODO reset input to create final graph chunks

    // sort the vertices by degree in ascending order
    // String degreeCounterFolder = internalWorkingDir.getAbsolutePath() +
    // File.separator
    // + "degreeCounter";
    // File sortedVertexFile = null;
    // try (RocksDBDegreeCounter degreeCounter = new
    // RocksDBDegreeCounter(degreeCounterFolder, 100);) {
    // for (Statement stmt : input) {
    // degreeCounter.countFor(stmt.getSubject());
    // degreeCounter.countFor(stmt.getObject());
    // }
    // sortedVertexFile = sortKeys(degreeCounter.iterator(), true,
    // internalWorkingDir, 1_000_000,
    // 100);
    // }

    // - vertex -> adjacent edges
    // - edge -> color
    // - join colors
    // - number of edges per color

    // - sort triple file by subject ids, thereby for each triple create
    // -- subject edgeId, object -edgeId
    // -- replace the predicate by edgIds (first triple has id 1, etc. inverse
    // -- count total number of edge #edges
    // edges have first two bytes = 0x0001)
    // - sort vertices by degree in ascending order (vertex, degree,
    // edgesIds+)* (number of edge ids can be too large for memory)

    // - recode edges by 1 for the first triple
    // - sortedVertexFile contains tuples (vertex,degree)
    // - use degree information to store |edge,edge,edge,edge|=degree in a
    // separate file

    // TODO Auto-generated method stub
    deleteFolder(internalWorkingDir);
    // TODO remove
    long requiredTime = System.currentTimeMillis() - start;
    System.out.println("required time: " + requiredTime);
  }

  private List<File> createInitialChunks(EncodedFileInputStream input, File workingDir,
          int numberOfCachedVertices) {
    List<File> chunks = new ArrayList<>();
    VertexIncidencentEdgesListFileCreator chunk = null;
    try {
      chunk = new VertexIncidencentEdgesListFileCreator(
              File.createTempFile("initialVertexIdChunk", "", workingDir));
      long nextEdgeId = 1;
      for (Statement stmt : input) {
        if ((chunk.getSize() >= numberOfCachedVertices) && (!chunk.contains(stmt.getSubjectAsLong())
                || !chunk.contains(stmt.getObjectAsLong()))) {
          chunk.flush();
          chunk.close();
          chunks.add(chunk.getFile());
          chunk = new VertexIncidencentEdgesListFileCreator(
                  File.createTempFile("initialVertexIdChunk", "", workingDir));
        }
        chunk.add(stmt.getSubjectAsLong(), nextEdgeId, true);
        chunk.add(stmt.getObjectAsLong(), nextEdgeId, false);
        nextEdgeId++;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (chunk != null) {
        chunk.flush();
        chunk.close();
        chunks.add(chunk.getFile());
      }
    }
    return chunks;
  }

  private File sortAndMerge(List<File> chunks, File workingDir, int maxNumberOfOpenFiles) {
    try {
      if (chunks.isEmpty()) {
        return File.createTempFile("sortedVertexIdChunk", "", workingDir);
      }
      while (chunks.size() > 1) {
        List<File> mergedChunks = new ArrayList<>();
        for (int iterationStart = 0; (chunks.size() > 1)
                && (iterationStart < chunks.size()); iterationStart += maxNumberOfOpenFiles - 1) {
          int numberOfProcessedFiles = Math.min(maxNumberOfOpenFiles - 1,
                  chunks.size() - iterationStart);
          EncodedLongFileInputStream[] inputs = new EncodedLongFileInputStream[numberOfProcessedFiles];
          File outputFile = File.createTempFile("sortedVertexIdChunk", "", workingDir);
          try (EncodedLongFileOutputStream output = new EncodedLongFileOutputStream(outputFile);) {
            EncodedLongFileInputIterator[] iterators = new EncodedLongFileInputIterator[numberOfProcessedFiles];
            long[] nextElements = new long[numberOfProcessedFiles];
            // initialize
            for (int i = 0; i < numberOfProcessedFiles; i++) {
              inputs[i] = new EncodedLongFileInputStream(chunks.get(iterationStart + i));
              iterators[i] = new EncodedLongFileInputIterator(inputs[i]);
              if (iterators[i].hasNext()) {
                nextElements[i] = iterators[i].next();
              } else {
                nextElements[i] = 0;
                iterators[i].close();
                iterators[i] = null;
              }
            }
            // merge
            for (BitSet indicesOfSmallestElement = getIndicesOfSmallestElement(
                    nextElements); !indicesOfSmallestElement
                            .isEmpty(); indicesOfSmallestElement = getIndicesOfSmallestElement(
                                    nextElements)) {
              // write vertexId
              output.writeLong(nextElements[indicesOfSmallestElement.nextSetBit(0)]);
              // collect out an in degrees
              long[] outDegrees = new long[indicesOfSmallestElement.cardinality()];
              long newOutDegree = 0;
              long[] inDegrees = new long[indicesOfSmallestElement.cardinality()];
              long newInDegree = 0;
              for (int i = indicesOfSmallestElement.nextSetBit(
                      0), k = 0; i >= 0; i = indicesOfSmallestElement.nextSetBit(i + 1), k++) {
                outDegrees[k] = iterators[i].next();
                newOutDegree += outDegrees[k];
                inDegrees[k] = iterators[i].next();
                newInDegree += inDegrees[k];
              }
              output.writeLong(newOutDegree);
              output.writeLong(newInDegree);
              // merge out-going edge lists
              for (int i = indicesOfSmallestElement.nextSetBit(
                      0), k = 0; i >= 0; i = indicesOfSmallestElement.nextSetBit(i + 1), k++) {
                for (int j = 0; j < outDegrees[k]; j++) {
                  output.writeLong(iterators[i].next());
                }
              }
              // merge in-going edge lists
              for (int i = indicesOfSmallestElement.nextSetBit(
                      0), k = 0; i >= 0; i = indicesOfSmallestElement.nextSetBit(i + 1), k++) {
                for (int j = 0; j < inDegrees[k]; j++) {
                  output.writeLong(iterators[i].next());
                }
              }
              // update next elements
              for (int i = indicesOfSmallestElement
                      .nextSetBit(0); i >= 0; i = indicesOfSmallestElement.nextSetBit(i + 1)) {
                if (iterators[i].hasNext()) {
                  nextElements[i] = iterators[i].next();
                } else {
                  nextElements[i] = 0;
                  iterators[i].close();
                  iterators[i] = null;
                }
              }
            }
            mergedChunks.add(outputFile);
          } finally {
            for (int i = 0; i < numberOfProcessedFiles; i++) {
              inputs[i].close();
              chunks.get(iterationStart + i).delete();
            }
          }
        }
        chunks = mergedChunks;
      }
      return chunks.get(0);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private BitSet getIndicesOfSmallestElement(long[] elements) {
    long smallestElement = 0;
    BitSet smallestIndices = new BitSet(elements.length);
    for (int i = 0; i < elements.length; i++) {
      if (elements[i] == 0) {
        continue;
      }
      if ((smallestElement == 0) || (elements[i] < smallestElement)) {
        smallestElement = elements[i];
        smallestIndices.clear();
        smallestIndices.set(i);
      } else if (elements[i] == smallestElement) {
        smallestIndices.set(i);
      }
    }
    return smallestIndices;
  }

  /**
   * @param elements
   * @param comparator
   * @return -1 if no smallest element exists
   */
  private int getIndexOfSmallestElement(long[][] elements, Comparator<long[]> comparator) {
    int smallest = -1;
    long[] smallestElement = null;
    for (int i = 0; i < elements.length; i++) {
      if (elements[i] == null) {
        continue;
      }
      if ((smallestElement == null) || (comparator.compare(smallestElement, elements[i]) > 0)) {
        smallest = i;
        smallestElement = elements[i];
      }
    }
    return smallest;
  }

  private File writeSortCacheFile(long[][] cache, int length, File workingDir) throws IOException {
    File outputFile = File.createTempFile("sortChunk", "", workingDir);
    try (EncodedLongFileOutputStream output = new EncodedLongFileOutputStream(outputFile);) {
      for (int i = 0; i < length; i++) {
        long[] entry = cache[i];
        for (long value : entry) {
          output.writeLong(value);
        }
      }
    }
    return outputFile;
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
    // TODO Auto-generated method stub
    super.close();
  }

  private void print(File vertexIncidentEdgesFile) {
    try (EncodedLongFileInputStream in = new EncodedLongFileInputStream(vertexIncidentEdgesFile);) {
      LongIterator iterator = in.iterator();
      while (iterator.hasNext()) {
        System.out.println("vertex" + iterator.next());
        long outDegree = iterator.next();
        long inDegree = iterator.next();
        System.out.print("\tout: " + outDegree + " edges = {");
        String delim = "";
        for (long i = 0; i < outDegree; i++) {
          System.out.print(delim + iterator.next());
          delim = ", ";
        }
        System.out.println("}");

        System.out.print("\tin: " + inDegree + " edges = {");
        delim = "";
        for (long i = 0; i < inDegree; i++) {
          System.out.print(delim + iterator.next());
          delim = ", ";
        }
        System.out.println("}");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
