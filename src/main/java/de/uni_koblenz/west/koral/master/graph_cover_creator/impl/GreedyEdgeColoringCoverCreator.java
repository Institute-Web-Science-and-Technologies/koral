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
import de.uni_koblenz.west.koral.master.utils.ReverseLongArrayComparator;
import de.uni_koblenz.west.koral.master.utils.RocksDBLongIterator;
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
import java.util.Arrays;
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

    List<File> initialChunks = createInitialChunks(input, internalWorkingDir,
            GreedyEdgeColoringCoverCreator.NUMBER_OF_CACHED_VERTICES);
    // File vertexIncidentEdgesFile = sortAndMerge(initialChunks);
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

  private List<File> createInitialChunks(EncodedFileInputStream input, File internalWorkingDir,
          int numberOfCachedVertices) {
    List<File> chunks = new ArrayList<>();
    VertexIncidencentEdgesListFileCreator chunk = null;
    try {
      chunk = new VertexIncidencentEdgesListFileCreator(
              File.createTempFile("InitialDegreeChunk", "", internalWorkingDir));
      long nextEdgeId = 1;
      for (Statement stmt : input) {
        if ((chunk.getSize() >= numberOfCachedVertices) && (!chunk.contains(stmt.getSubjectAsLong())
                || !chunk.contains(stmt.getObjectAsLong()))) {
          chunk.flush();
          chunk.close();
          chunks.add(chunk.getFile());
          chunk = new VertexIncidencentEdgesListFileCreator(
                  File.createTempFile("InitialDegreeChunk", "", internalWorkingDir));
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

  private File sortKeys(RocksDBLongIterator iterator, boolean ascendigOrder, File workingDir,
          int numberOfCachedElements, int numberOfMergedFiles) {
    Comparator<long[]> comparator = new ReverseLongArrayComparator(ascendigOrder);
    try {
      List<File> sortFiles = new ArrayList<>();
      // create initial chunks
      long[][] cache = new long[numberOfCachedElements][];
      int nextIndex = 0;
      while (iterator.hasNext()) {
        long[] entry = iterator.next();
        cache[nextIndex++] = entry;
        if (nextIndex == cache.length) {
          Arrays.sort(cache, 0, nextIndex, comparator);
          sortFiles.add(writeSortCacheFile(cache, nextIndex, workingDir));
          nextIndex = 0;
        }
      }
      if (nextIndex > 0) {
        Arrays.sort(cache, 0, nextIndex, comparator);
        sortFiles.add(writeSortCacheFile(cache, nextIndex, workingDir));
      }
      if (sortFiles.isEmpty()) {
        return File.createTempFile("sortChunk", "", workingDir);
      }
      // merge chunks
      while (sortFiles.size() > 1) {
        List<File> mergedChunks = new ArrayList<>();
        for (int iterationStart = 0; (sortFiles.size() > 1)
                && (iterationStart < sortFiles.size()); iterationStart += numberOfMergedFiles) {
          int numberOfProcessedFiles = Math.min(numberOfMergedFiles,
                  sortFiles.size() - iterationStart);
          EncodedLongFileInputStream[] inputs = new EncodedLongFileInputStream[numberOfProcessedFiles];
          File outputFile = File.createTempFile("sortChunk", "", workingDir);
          try (EncodedLongFileOutputStream output = new EncodedLongFileOutputStream(outputFile);) {
            EncodedLongFileInputIterator[] iterators = new EncodedLongFileInputIterator[numberOfProcessedFiles];
            long[][] nextElements = new long[numberOfProcessedFiles][];
            for (int i = 0; i < numberOfProcessedFiles; i++) {
              inputs[i] = new EncodedLongFileInputStream(sortFiles.get(iterationStart + i));
              iterators[i] = new EncodedLongFileInputIterator(inputs[i]);
              if (iterators[i].hasNext()) {
                nextElements[i] = new long[] { iterators[i].next(), iterators[i].next() };
              } else {
                nextElements[i] = null;
              }
            }
            for (int smallest = getIndexOfSmallestElement(nextElements,
                    comparator); smallest >= 0; smallest = getIndexOfSmallestElement(nextElements,
                            comparator)) {
              output.writeLong(nextElements[smallest][0]);
              output.writeLong(nextElements[smallest][1]);
              if (iterators[smallest].hasNext()) {
                nextElements[smallest] = new long[] { iterators[smallest].next(),
                        iterators[smallest].next() };
              } else {
                nextElements[smallest] = null;
              }
            }
            mergedChunks.add(outputFile);
          } finally {
            for (int i = 0; i < numberOfProcessedFiles; i++) {
              inputs[i].close();
              sortFiles.get(iterationStart + i).delete();
            }
          }
        }
        sortFiles = mergedChunks;
      }
      return sortFiles.get(0);
      // // remove the frequencies of the file
      // File outputFile = File.createTempFile("sortedVertices", "",
      // workingDir);
      // try (EncodedLongFileOutputStream output = new
      // EncodedLongFileOutputStream(outputFile);
      // EncodedLongFileInputStream input = new EncodedLongFileInputStream(
      // sortFiles.get(0));) {
      // LongIterator iter = input.iterator();
      // while (iter.hasNext()) {
      // output.writeLong(iter.next());
      // // skip frequency
      // iter.next();
      // }
      // iter.close();
      // }
      // return outputFile;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      iterator.close();
    }
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

}
