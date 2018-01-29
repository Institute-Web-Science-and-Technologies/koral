package de.uni_koblenz.west.koral.master.graph_cover_creator.impl;

import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedFileOutputStream;
import de.uni_koblenz.west.koral.common.io.EncodedLongFileInputIterator;
import de.uni_koblenz.west.koral.common.io.EncodedLongFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedLongFileOutputStream;
import de.uni_koblenz.west.koral.common.io.EncodedRandomAccessLongFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedRandomAccessLongFileOutputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.LongOutputWriter;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.koral.master.utils.ColoringManager;
import de.uni_koblenz.west.koral.master.utils.EdgeArrayIterator;
import de.uni_koblenz.west.koral.master.utils.EdgeFileIterator;
import de.uni_koblenz.west.koral.master.utils.EdgeIterator;
import de.uni_koblenz.west.koral.master.utils.FixedSizeLongArrayComparator;
import de.uni_koblenz.west.koral.master.utils.InitialChunkProducer;
import de.uni_koblenz.west.koral.master.utils.LongIterator;
import de.uni_koblenz.west.koral.master.utils.Merger;
import de.uni_koblenz.west.koral.master.utils.NWayMergeSort;
import de.uni_koblenz.west.koral.master.utils.VertexIncidentEdgesDegreeComparator;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Creates a greedy edge coloring cover.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class GreedyEdgeColoringCoverCreator extends GraphCoverCreatorBase {

  // TODO adjust
  private static final int NUMBER_OF_CACHED_VERTICES = 0x10_00_00;

  // TODO determine by available memory
  private static final int NUMBER_OF_CACHED_EDGES = 0x04_00_00_00;

  private static final int MAX_NUMBER_OF_OPEN_FILES = 100;

  private long numberOfEdges = 0;

  public GreedyEdgeColoringCoverCreator(Logger logger, MeasurementCollector measurementCollector) {
    super(logger, measurementCollector);
    // if (Runtime.getRuntime().maxMemory() == Long.MAX_VALUE) {
    // NUMBER_OF_CACHED_EDGES = 0x04_00_00_00;
    // } else {
    // long allocatedMemory = Runtime.getRuntime().totalMemory() -
    // Runtime.getRuntime().freeMemory();
    // long presumableFreeMemory = Runtime.getRuntime().maxMemory() -
    // allocatedMemory;
    // NUMBER_OF_CACHED_EDGES = (int) (presumableFreeMemory / 8 / 2);
    // }
    // System.out.println("ram " + NUMBER_OF_CACHED_EDGES);
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
    // TODO remove
    long transformStart = System.currentTimeMillis();
    File vertexIncidentEdgesFile = createVertexIncidentEdgesFile(input, internalWorkingDir,
            GreedyEdgeColoringCoverCreator.NUMBER_OF_CACHED_VERTICES,
            GreedyEdgeColoringCoverCreator.MAX_NUMBER_OF_OPEN_FILES);
    // TODO remove
    System.out.println("transformation time: " + (System.currentTimeMillis() - transformStart));

    // long sortStart = System.currentTimeMillis();
    //
    // // sort by degree
    // File[] verticesByDegreeLevels =
    // splitIntoDegreeLevels(vertexIncidentEdgesFile,
    // internalWorkingDir,
    // GreedyEdgeColoringCoverCreator.NUMBER_OF_CACHED_VERTICES,
    // GreedyEdgeColoringCoverCreator.MAX_NUMBER_OF_OPEN_FILES);
    // // TODO remove
    // System.out.println("spliting by degree time: " +
    // (System.currentTimeMillis() - sortStart));
    // sortStart = System.currentTimeMillis();
    // File sortedVertexList = null;
    // for (int level = 0; level < verticesByDegreeLevels.length; level++) {
    // sortedVertexList = sort(level, verticesByDegreeLevels[level],
    // sortedVertexList,
    // internalWorkingDir,
    // GreedyEdgeColoringCoverCreator.NUMBER_OF_CACHED_VERTICES,
    // GreedyEdgeColoringCoverCreator.NUMBER_OF_CACHED_EDGES,
    // GreedyEdgeColoringCoverCreator.MAX_NUMBER_OF_OPEN_FILES);
    // }
    // // TODO remove
    // System.out.println("sort time: " + (System.currentTimeMillis() -
    // sortStart));
    //
    // File edges2chunks = null;
    // try (ColoringManager colorManager = new
    // ColoringManager(internalWorkingDir,
    // GreedyEdgeColoringCoverCreator.MAX_NUMBER_OF_OPEN_FILES / 2);) {
    // // create edge coloring
    // createEdgeColoring(sortedVertexList, colorManager, internalWorkingDir,
    // numberOfEdges,
    // numberOfGraphChunks,
    // GreedyEdgeColoringCoverCreator.NUMBER_OF_CACHED_EDGES,
    // GreedyEdgeColoringCoverCreator.MAX_NUMBER_OF_OPEN_FILES);
    // // assign edges to graph chunks
    // edges2chunks = createAssignmentOfEdgesToChunks(colorManager,
    // internalWorkingDir,
    // numberOfEdges, numberOfGraphChunks,
    // GreedyEdgeColoringCoverCreator.NUMBER_OF_CACHED_EDGES,
    // GreedyEdgeColoringCoverCreator.MAX_NUMBER_OF_OPEN_FILES / 2);
    // }

    // sort edges2chunks by edgeIds in ascending order
    // output is a list of partitionIds: e1->4;e2->2 is stored as [4,2]

    // iterate partitionIds and input in parallel to translate edgeId back into
    // triple

    // TODO reset input to create final graph chunks

    print(vertexIncidentEdgesFile);

    // TODO Auto-generated method stub
    deleteFolder(internalWorkingDir);
    // TODO remove
    long requiredTime = System.currentTimeMillis() - start;
    System.out.println("required time: " + requiredTime);
  }

  private File createAssignmentOfEdgesToChunks(ColoringManager colorManager, File workingDir,
          long numberOfEdges, int numberOfGraphChunks, int numberOfCachedEdges,
          int maxNumberOfOpenFiles) {
    try {
      Iterator<long[]> iteratorOverColors = colorManager.getIteratorOverAllColors();
      File colorsSortedBySizeDesc = sortColors(iteratorOverColors, workingDir, numberOfCachedEdges,
              maxNumberOfOpenFiles);
      long[] chunkSizes = new long[numberOfGraphChunks];
      File colors2chunks = File.createTempFile("colors2chunks", "", workingDir);
      // sort colors by size in descending order

      // perform greedy algorithm to assign colors to chunk

      // when assigning color to a chunk, write edgeID,chunkID to the output
      // file
      // TODO Auto-generated method stub
      return null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private File sortColors(Iterator<long[]> iteratorOverColors, File workingDir,
          int numberOfCachedEdges, int maxNumberOfOpenFiles) {
    // TODO Auto-generated method stub
    return null;
  }

  private void createEdgeColoring(File sortedVertexList, ColoringManager colorManager,
          File workingDir, long numberOfEdges, int numberOfGraphChunks, int numberOfCachedEdges,
          int maxNumberOfOpenFiles) {
    long maxNumberOfEdgesPerColour = numberOfEdges / numberOfGraphChunks;
    long maxInMemoryEdgeNumber = numberOfCachedEdges / 3;
    if (maxInMemoryEdgeNumber > (65536 / 3)) {
      maxInMemoryEdgeNumber = 65536 / 3;
    }
    /*
     * edges[i][0]=edgeId, edges[i][1]=colorId, edges[i][2]=sizeOfColor
     */
    long[][] edges = new long[1][3];
    try (EncodedLongFileInputStream input = new EncodedLongFileInputStream(sortedVertexList);
            LongIterator iterator = input.iterator();) {
      while (iterator.hasNext()) {
        @SuppressWarnings("unused")
        long vertexId = iterator.next();
        long outDegree = iterator.next();
        long inDegree = iterator.next();
        long degree = outDegree + inDegree;
        if (degree <= maxInMemoryEdgeNumber) {
          if ((edges == null) || (degree > edges.length)) {
            edges = new long[(int) degree][3];
          }
          for (int i = 0; i < degree; i++) {
            edges[i][0] = iterator.next();
          }
          colorManager.fillColorInformation(edges);
          try (EdgeIterator iter = new EdgeArrayIterator(edges);) {
            colourEdges(iter, colorManager, maxNumberOfEdgesPerColour);
          }
        } else {
          edges = null;
          File edgeColors = getEdgeColors(iterator, outDegree, inDegree, colorManager, workingDir,
                  (int) maxInMemoryEdgeNumber, (maxNumberOfOpenFiles / 2) - 1);
          try (EdgeIterator iter = new EdgeFileIterator(edgeColors);) {
            colourEdges(iter, colorManager, maxNumberOfEdgesPerColour);
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void colourEdges(EdgeIterator edges, ColoringManager colorManager,
          long maxNumberOfEdgesPerColour) {
    /*
     * color[0] = colorId; color[1] = frequency
     */
    List<long[]> colorCache = new LinkedList<>();
    // TODO check number of cached colors
    long[] edge = edges.next();
    // iterate over all colored edges
    while ((edge[1] != 0) && edges.hasNext()) {
      if (edge[2] == maxNumberOfEdgesPerColour) {
        // the edge is full, thus it cannot be used for anything
      } else if (colorCache.isEmpty()) {
        // this is the first color
        colorCache.add(new long[] { edge[1], edge[2] });
      } else {
        long[] last = colorCache.get(colorCache.size() - 1);
        if (last[0] == edge[1]) {
          // this color existed already, thus there is nothing to do
        } else {
          // find join candidate with which this color can be joined
          long[] joinedColor = null;
          for (long[] color : colorCache) {
            if ((color[1] + edge[2]) <= maxNumberOfEdgesPerColour) {
              // recolor current edge
              colorManager.recolor(edge[1], edge[2], color[0], color[1]);
              color[1] += edge[2];
              joinedColor = color;
              break;
            }
          }
          if (joinedColor == null) {
            // this color could not be joined
            colorCache.add(new long[] { edge[1], edge[2] });
          } else if (joinedColor[1] == maxNumberOfEdgesPerColour) {
            // the joined color is full and can be removed
            colorCache.remove(joinedColor);
          }
        }
      }
      // read next edge
      edge = edges.next();
    }
    // sort colors by frequency
    Collections.sort(colorCache, new FixedSizeLongArrayComparator(false, 1));
    // iterate over all uncolored edges
    while (edges.hasNext()) {
      long[] color = null;
      if (colorCache.isEmpty()) {
        // create a new color
        color = colorManager.createNewColor();
        colorCache.add(color);
      } else {
        // use largest color to color the new edge
        color = colorCache.get(0);
      }
      colorManager.colorEdge(edge[0], color[0]);
      color[1]++;
      if (color[1] >= maxNumberOfEdgesPerColour) {
        // the color is full and cannot be used any more
        colorCache.remove(color);
      }
      // read next edge
      edge = edges.next();
    }
  }

  private File getEdgeColors(LongIterator iterator, long outDegree, long inDegree,
          ColoringManager colorManager, File workingDir, int maxInMemoryEdgeNumber,
          int maxNumberOfOpenFiles) throws IOException {
    Comparator<long[]> comparator = new FixedSizeLongArrayComparator(false, 2);
    List<File> edgeColorChunks = new ArrayList<>();
    // create initial merge sort chunks
    long[][] cachedEdges = new long[maxInMemoryEdgeNumber][3];
    int nextIndex = 0;
    long numberOfEdgesToRead = outDegree + inDegree;
    for (long i = numberOfEdgesToRead; i > 0; i--) {
      if (nextIndex == cachedEdges.length) {
        writeEdgeColorChunk(colorManager, workingDir, comparator, edgeColorChunks, cachedEdges,
                nextIndex);
        nextIndex = 0;
      }
      cachedEdges[nextIndex][0] = iterator.next();
      nextIndex++;
    }
    if (nextIndex > 0) {
      writeEdgeColorChunk(colorManager, workingDir, comparator, edgeColorChunks, cachedEdges,
              nextIndex);
    }
    cachedEdges = null;
    // perform merge sort
    while (edgeColorChunks.size() > 1) {
      List<File> nextChunks = new ArrayList<>();
      for (int iterationStart = 0; (edgeColorChunks.size() > 1)
              && (iterationStart < edgeColorChunks.size()); iterationStart += maxNumberOfOpenFiles
                      - 1) {
        int numberOfProcessedFiles = Math.min(maxNumberOfOpenFiles - 1,
                edgeColorChunks.size() - iterationStart);
        EncodedRandomAccessLongFileInputStream[] inputs = new EncodedRandomAccessLongFileInputStream[numberOfProcessedFiles];
        File outputFile = File.createTempFile("mergeSortChunk", "", workingDir);
        nextChunks.add(outputFile);
        try (EncodedLongFileOutputStream output = new EncodedLongFileOutputStream(outputFile);) {
          long[][] edges = new long[numberOfProcessedFiles][];
          // initialize
          for (int i = 0; i < inputs.length; i++) {
            inputs[i] = new EncodedRandomAccessLongFileInputStream(
                    edgeColorChunks.get(iterationStart + i));
            edges[i] = new long[] { inputs[i].readLong(), inputs[i].readLong(),
                    inputs[i].readLong() };
          }
          // merge
          for (BitSet indicesOfSmallestElement = getIndicesOfSmallestElement(edges,
                  comparator); !indicesOfSmallestElement
                          .isEmpty(); indicesOfSmallestElement = getIndicesOfSmallestElement(edges,
                                  comparator)) {
            int smallesElementIndex = indicesOfSmallestElement.nextSetBit(0);
            long[] edge = edges[smallesElementIndex];
            if (edge[1] == 0) {
              // these are uncolored edges
              for (int i = smallesElementIndex; i >= 0; i = indicesOfSmallestElement
                      .nextSetBit(i + 1)) {
                output.writeLong(edges[i][0]);
                output.writeLong(edges[i][1]);
                output.writeLong(edges[i][2]);
                try {
                  edges[i][0] = inputs[i].readLong();
                  edges[i][1] = inputs[i].readLong();
                  edges[i][2] = inputs[i].readLong();
                } catch (EOFException e) {
                  edges[i] = null;
                }
              }
            } else {
              output.writeLong(edge[0]);
              output.writeLong(edge[1]);
              output.writeLong(edge[2]);
              try {
                edges[smallesElementIndex][0] = inputs[smallesElementIndex].readLong();
                edges[smallesElementIndex][1] = inputs[smallesElementIndex].readLong();
                edges[smallesElementIndex][2] = inputs[smallesElementIndex].readLong();
              } catch (EOFException e) {
                edges[smallesElementIndex] = null;
              }
            }
          }
        } finally {
          for (int i = 0; i < numberOfProcessedFiles; i++) {
            inputs[i].close();
            edgeColorChunks.get(iterationStart + i).delete();
          }
        }
      }
      edgeColorChunks = nextChunks;
    }
    return edgeColorChunks.get(0);
  }

  private BitSet getIndicesOfSmallestElement(long[][] elements, Comparator<long[]> comparator) {
    long[] smallestElement = null;
    BitSet smallestIndices = new BitSet(elements.length);
    for (int i = 0; i < elements.length; i++) {
      if (elements[i] == null) {
        continue;
      }
      if ((smallestElement == null) || ((comparator.compare(elements[i], smallestElement)) < 0)) {
        smallestElement = elements[i];
        smallestIndices.clear();
        smallestIndices.set(i);
      } else if (elements[i][2] == smallestElement[2]) {
        smallestIndices.set(i);
      }
    }
    return smallestIndices;
  }

  private void writeEdgeColorChunk(ColoringManager colorManager, File workingDir,
          Comparator<long[]> comparator, List<File> edgeColorChunks, long[][] cachedEdges,
          int nextIndex) throws IOException, FileNotFoundException {
    colorManager.fillColorInformation(cachedEdges, nextIndex);
    Arrays.sort(cachedEdges, 0, nextIndex, comparator);
    File output = File.createTempFile("edgeColorInitialChunk", "", workingDir);
    try (EncodedRandomAccessLongFileOutputStream out = new EncodedRandomAccessLongFileOutputStream(
            output);) {
      long previousColor = 0;
      for (int j = 0; j < nextIndex; j++) {
        if ((cachedEdges[j][1] == 0) || (cachedEdges[j][1] != previousColor)) {
          out.writeLong(cachedEdges[j][0]);
          out.writeLong(cachedEdges[j][1]);
          out.writeLong(cachedEdges[j][2]);
          previousColor = cachedEdges[j][1];
        }
      }
    }
    edgeColorChunks.add(output);
  }

  private File sort(int level, File vertexFile, File outputFile, File workingDir,
          int numberOfCachedVertices, int numberOfCachedEdges, int maxNumberOfOpenFiles) {
    long maxDegree = getUpperLimitOfLevel(level, numberOfCachedVertices, maxNumberOfOpenFiles);
    if (maxDegree == 1) {
      return vertexFile;
    } else {
      if ((vertexFile.length() == 0) || (vertexFile.length() == 20)) {
        vertexFile.delete();
        return outputFile;
      }
      if (maxDegree == 2) {
        // append vertices with degree 2
        appendFile(vertexFile, outputFile);
        vertexFile.delete();
        return outputFile;
      } else {
        int numberOfCachedVerticesPerLevel = (int) (numberOfCachedEdges / maxDegree);
        if (numberOfCachedVerticesPerLevel > 65536) {
          numberOfCachedVerticesPerLevel = 65536;
        }
        if (numberOfCachedVerticesPerLevel == 0) {
          File edgeListFile = null;
          try {
            edgeListFile = File.createTempFile("edgeListFile-", "", workingDir);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          List<File> initialChunks = createInitialLongEdgeChunks(vertexFile, workingDir,
                  edgeListFile, numberOfCachedVertices > 65536 ? 65536 : numberOfCachedVertices);
          File fileWithoutEdges = merge(initialChunks, workingDir, maxNumberOfOpenFiles, false);
          appendAndMerge(fileWithoutEdges, edgeListFile, outputFile);
          fileWithoutEdges.delete();
          edgeListFile.delete();
        } else {
          // perform merge sort
          List<File> initialChunks = createInitialChunks(vertexFile, workingDir,
                  numberOfCachedVerticesPerLevel);
          File sortedFile = merge(initialChunks, workingDir, maxNumberOfOpenFiles, true);
          appendFile(sortedFile, outputFile);
          sortedFile.delete();
          vertexFile.delete();
        }
        return outputFile;
      }
    }
  }

  private List<File> createInitialLongEdgeChunks(File vertexFile, File workingDir,
          File edgeListFile, int numberOfCachedVertices) {
    List<File> result = new ArrayList<>();
    LongIterator iterator = null;
    try (EncodedLongFileInputStream input = new EncodedLongFileInputStream(vertexFile);
            EncodedRandomAccessLongFileOutputStream edgeListOut = new EncodedRandomAccessLongFileOutputStream(
                    edgeListFile);) {
      iterator = input.iterator();
      long[][] vertexHeaders = new long[numberOfCachedVertices][4];
      while (iterator.hasNext()) {
        // load data in memory
        int nextIndex = 0;
        while ((nextIndex < vertexHeaders.length) && iterator.hasNext()) {
          vertexHeaders[nextIndex][0] = iterator.next();
          vertexHeaders[nextIndex][1] = iterator.next();
          vertexHeaders[nextIndex][2] = iterator.next();
          vertexHeaders[nextIndex][3] = edgeListOut.getPosition();
          for (int i = 0; i < vertexHeaders[nextIndex][1]; i++) {
            edgeListOut.writeLong(iterator.next());
          }
          for (int i = 0; i < vertexHeaders[nextIndex][2]; i++) {
            edgeListOut.writeLong(iterator.next());
          }
          nextIndex++;
        }
        Arrays.sort(vertexHeaders, 0, nextIndex,
                VertexIncidentEdgesDegreeComparator.getComparator(true));
        File outputFile = File.createTempFile("initialMergeSortChunk", "", workingDir);
        result.add(outputFile);
        try (EncodedLongFileOutputStream output = new EncodedLongFileOutputStream(outputFile,
                true);) {
          for (int i = 0; i < nextIndex; i++) {
            output.writeLong(vertexHeaders[i][0]);
            output.writeLong(vertexHeaders[i][1]);
            output.writeLong(vertexHeaders[i][2]);
            output.writeLong(vertexHeaders[i][3]);
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (iterator != null) {
        iterator.close();
      }
    }
    return result;
  }

  private File merge(List<File> chunks, File workingDir, int maxNumberOfOpenFiles,
          boolean containsFullEdgeLists) {
    try {
      if (chunks.isEmpty()) {
        return File.createTempFile("mergeSortChunk", "", workingDir);
      }
      while (chunks.size() > 1) {
        List<File> mergedChunks = new ArrayList<>();
        for (int iterationStart = 0; (chunks.size() > 1)
                && (iterationStart < chunks.size()); iterationStart += maxNumberOfOpenFiles - 1) {
          int numberOfProcessedFiles = Math.min(maxNumberOfOpenFiles - 1,
                  chunks.size() - iterationStart);
          EncodedLongFileInputStream[] inputs = new EncodedLongFileInputStream[numberOfProcessedFiles];
          File outputFile = File.createTempFile("mergeSortChunk", "", workingDir);
          try (EncodedLongFileOutputStream output = new EncodedLongFileOutputStream(outputFile);) {
            EncodedLongFileInputIterator[] iterators = new EncodedLongFileInputIterator[numberOfProcessedFiles];
            long[][] nextElements = new long[numberOfProcessedFiles][3];
            // initialize
            for (int i = 0; i < numberOfProcessedFiles; i++) {
              inputs[i] = new EncodedLongFileInputStream(chunks.get(iterationStart + i));
              iterators[i] = new EncodedLongFileInputIterator(inputs[i]);
              if (iterators[i].hasNext()) {
                nextElements[i] = new long[] { iterators[i].next(), iterators[i].next(),
                        iterators[i].next() };
              } else {
                nextElements[i] = null;
                iterators[i].close();
                iterators[i] = null;
              }
            }
            // merge
            for (int nextIndex = getIndexOfSmallestElement(nextElements,
                    VertexIncidentEdgesDegreeComparator.getComparator(
                            true)); nextIndex != -1; nextIndex = getIndexOfSmallestElement(
                                    nextElements,
                                    VertexIncidentEdgesDegreeComparator.getComparator(true))) {
              output.writeLong(nextElements[nextIndex][0]);
              output.writeLong(nextElements[nextIndex][1]);
              output.writeLong(nextElements[nextIndex][2]);
              if (containsFullEdgeLists) {
                // copy edge lists
                for (int i = 0; i < nextElements[nextIndex][1]; i++) {
                  output.writeLong(iterators[nextIndex].next());
                }
                for (int i = 0; i < nextElements[nextIndex][2]; i++) {
                  output.writeLong(iterators[nextIndex].next());
                }
              } else {
                // copy position in edge file
                output.writeLong(iterators[nextIndex].next());
              }
              if (iterators[nextIndex].hasNext()) {
                nextElements[nextIndex] = new long[] { iterators[nextIndex].next(),
                        iterators[nextIndex].next(), iterators[nextIndex].next() };
              } else {
                nextElements[nextIndex] = null;
                iterators[nextIndex].close();
                iterators[nextIndex] = null;
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

  private List<File> createInitialChunks(File vertexFile, File workingDir,
          int numberOfCachedVerticesPerLevel) {
    List<File> result = new ArrayList<>();
    LongIterator iterator = null;
    try (EncodedLongFileInputStream input = new EncodedLongFileInputStream(vertexFile);) {
      iterator = input.iterator();
      long[][] vertexHeaders = new long[numberOfCachedVerticesPerLevel][4];
      long[][] outEdges = new long[numberOfCachedVerticesPerLevel][];
      long[][] inEdges = new long[numberOfCachedVerticesPerLevel][];
      while (iterator.hasNext()) {
        // load data in memory
        int nextIndex = 0;
        while ((nextIndex < vertexHeaders.length) && iterator.hasNext()) {
          vertexHeaders[nextIndex][0] = iterator.next();
          vertexHeaders[nextIndex][1] = iterator.next();
          vertexHeaders[nextIndex][2] = iterator.next();
          vertexHeaders[nextIndex][3] = nextIndex;
          outEdges[nextIndex] = readEdges(outEdges[nextIndex], iterator,
                  (int) vertexHeaders[nextIndex][1]);
          inEdges[nextIndex] = readEdges(inEdges[nextIndex], iterator,
                  (int) vertexHeaders[nextIndex][2]);
          nextIndex++;
        }
        Arrays.sort(vertexHeaders, 0, nextIndex,
                VertexIncidentEdgesDegreeComparator.getComparator(true));
        File outputFile = File.createTempFile("initialMergeSortChunk", "", workingDir);
        result.add(outputFile);
        try (EncodedLongFileOutputStream output = new EncodedLongFileOutputStream(outputFile,
                true);) {
          for (int i = 0; i < nextIndex; i++) {
            output.writeLong(vertexHeaders[i][0]);
            output.writeLong(vertexHeaders[i][1]);
            output.writeLong(vertexHeaders[i][2]);
            int edgeIndex = (int) vertexHeaders[i][3];
            for (int j = 0; j < vertexHeaders[i][1]; j++) {
              output.writeLong(outEdges[edgeIndex][j]);
            }
            for (int j = 0; j < vertexHeaders[i][2]; j++) {
              output.writeLong(inEdges[edgeIndex][j]);
            }
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (iterator != null) {
        iterator.close();
      }
    }
    return result;
  }

  private long[] readEdges(long[] result, LongIterator iterator, int numberOfEdges) {
    if ((result == null) || (numberOfEdges >= result.length)) {
      result = new long[numberOfEdges];
    }
    for (int i = 0; i < result.length; i++) {
      result[i] = i < numberOfEdges ? iterator.next() : 0;
    }
    return result;
  }

  private void appendFile(File vertexFile, File outputFile) {
    LongIterator iterator = null;
    try (EncodedLongFileInputStream input = new EncodedLongFileInputStream(vertexFile);
            EncodedLongFileOutputStream output = new EncodedLongFileOutputStream(outputFile,
                    true);) {
      iterator = input.iterator();
      while (iterator.hasNext()) {
        output.writeLong(iterator.next());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (iterator != null) {
        iterator.close();
      }
    }
  }

  private void appendAndMerge(File vertexFile, File edgeListFile, File outputFile) {
    LongIterator iterator = null;
    try (EncodedLongFileInputStream input = new EncodedLongFileInputStream(vertexFile);
            EncodedRandomAccessLongFileInputStream edgeInput = new EncodedRandomAccessLongFileInputStream(
                    edgeListFile);
            EncodedLongFileOutputStream output = new EncodedLongFileOutputStream(outputFile,
                    true);) {
      iterator = input.iterator();
      while (iterator.hasNext()) {
        // vertex id
        output.writeLong(iterator.next());
        long outDegree = iterator.next();
        output.writeLong(outDegree);
        long inDegree = iterator.next();
        output.writeLong(inDegree);
        long offset = iterator.next();
        edgeInput.setPosition(offset);
        for (int i = 0; i < outDegree; i++) {
          output.writeLong(edgeInput.readLong());
        }
        for (int i = 0; i < inDegree; i++) {
          output.writeLong(edgeInput.readLong());
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (iterator != null) {
        iterator.close();
      }
    }
  }

  private File[] splitIntoDegreeLevels(File vertexIncidentEdgesFile, File workingDir,
          int numberOfCachedVertices, int maxNumberOfOpenFiles) {
    int numberOfLevels = Integer.SIZE - Integer.numberOfLeadingZeros(numberOfCachedVertices);
    if (numberOfLevels > (maxNumberOfOpenFiles - 1)) {
      numberOfLevels = maxNumberOfOpenFiles - 1;
    }
    File[] levels = new File[numberOfLevels];
    EncodedLongFileOutputStream[] outputs = new EncodedLongFileOutputStream[numberOfLevels];
    try {
      for (int i = 0; i < levels.length; i++) {
        String prefix = i == 0 ? "sortedVertexList-"
                : "verticesWithDegrees-"
                        + getLowerLimitOfLevel(i, numberOfCachedVertices, maxNumberOfOpenFiles)
                        + "to"
                        + getUpperLimitOfLevel(i, numberOfCachedVertices, maxNumberOfOpenFiles)
                        + "-";
        levels[i] = File.createTempFile(prefix, "", workingDir);
        outputs[i] = new EncodedLongFileOutputStream(levels[i]);
      }
      LongIterator iterator = null;
      try (EncodedLongFileInputStream input = new EncodedLongFileInputStream(
              vertexIncidentEdgesFile);) {
        iterator = input.iterator();
        while (iterator.hasNext()) {
          long vertexId = iterator.next();
          long outDegree = iterator.next();
          long inDegree = iterator.next();
          int level = getInsertionLevel(outDegree + inDegree, numberOfCachedVertices,
                  maxNumberOfOpenFiles);
          outputs[level].writeLong(vertexId);
          outputs[level].writeLong(outDegree);
          outputs[level].writeLong(inDegree);
          for (int i = 0; i < outDegree; i++) {
            outputs[level].writeLong(iterator.next());
          }
          for (int i = 0; i < inDegree; i++) {
            outputs[level].writeLong(iterator.next());
          }
        }
      } finally {
        if (iterator != null) {
          iterator.close();
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      for (EncodedLongFileOutputStream output : outputs) {
        if (output != null) {
          try {
            output.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    }
    return levels;
  }

  private int getInsertionLevel(long degree, int numberOfCachedVertices, int maxNumberOfOpenFiles) {
    int level = 0;
    while (degree > getUpperLimitOfLevel(level, numberOfCachedVertices, maxNumberOfOpenFiles)) {
      level++;
    }
    return level;
  }

  private long getLowerLimitOfLevel(int level, int numberOfCachedVertices,
          int maxNumberOfOpenFiles) {
    return getUpperLimitOfLevel(level - 1, numberOfCachedVertices, maxNumberOfOpenFiles) + 1;
  }

  private long getUpperLimitOfLevel(int level, int numberOfCachedVertices,
          int maxNumberOfOpenFiles) {
    int numberOfLevels = Integer.SIZE - Integer.numberOfLeadingZeros(numberOfCachedVertices);
    if (numberOfLevels > (maxNumberOfOpenFiles - 1)) {
      numberOfLevels = maxNumberOfOpenFiles - 1;
    }
    if (level == (numberOfLevels - 1)) {
      return Long.MAX_VALUE;
    } else {
      return numberOfCachedVertices >>> (numberOfLevels - level - 1);
    }
  }

  private File createVertexIncidentEdgesFile(EncodedFileInputStream input, File workingDir,
          int numberOfCachedVertices, int maxNumberOfOpenFiles) {
    InitialChunkProducer producer = null;
    try {
      producer = new InitialChunkProducer() {

        private static final int INITIAL_ARRAY_SIZE = 1;

        private Map<Long, Integer> vertexId2Index = new HashMap<>();

        /**
         * stores for vertex i:
         * vertexDegrees[i]={vertexId,outdegree,indegree,incidentEdgesIndex}
         */
        private List<long[]> vertexDegrees = new ArrayList<>();

        /**
         * stores for vertex i: incidentEdges[vertexDegrees[i][3]][0] = list of
         * outgoing edges, incidentEdges[vertexDegrees[i][3]][1] list of ingoing
         * edges
         */
        private List<long[][]> incidentEdges = new ArrayList<>();

        private long nextEdgeId = 1;

        private final Iterator<Statement> iterator = input.iterator();

        @Override
        public void loadNextChunk() {
          clear();
          while (iterator.hasNext()) {
            if (vertexId2Index.size() >= numberOfCachedVertices) {
              return;
            }
            Statement stmt = iterator.next();
            add(stmt.getSubjectAsLong(), nextEdgeId, true);
            add(stmt.getObjectAsLong(), nextEdgeId, false);
            nextEdgeId++;
          }
        }

        private void add(long vertexId, long edgeId, boolean isOutgoingEdge) {
          Integer index = vertexId2Index.get(vertexId);
          long[] vertexDegree = null;
          long[][] incidentEdge = null;
          if (index == null) {
            index = Integer.valueOf(vertexId2Index.size());
            vertexId2Index.put(vertexId, index);
            if (vertexDegrees.size() <= index) {
              vertexDegree = new long[] { vertexId, 0, 0, index };
              vertexDegrees.add(vertexDegree);
              incidentEdge = new long[2][INITIAL_ARRAY_SIZE];
              incidentEdges.add(incidentEdge);
            } else {
              vertexDegree = vertexDegrees.get(index);
              vertexDegree[0] = vertexId;
              vertexDegree[1] = 0;
              vertexDegree[2] = 0;
              vertexDegree[3] = index;
              incidentEdge = incidentEdges.get(index);
            }
          } else {
            vertexDegree = vertexDegrees.get(index);
            incidentEdge = incidentEdges.get((int) vertexDegree[3]);
          }
          incidentEdge[isOutgoingEdge ? 0 : 1] = addEdge(edgeId,
                  incidentEdge[isOutgoingEdge ? 0 : 1], (int) vertexDegree[isOutgoingEdge ? 1 : 2]);
          vertexDegree[isOutgoingEdge ? 1 : 2]++;
        }

        private long[] addEdge(long edgeId, long[] edgeList, int insertionIndex) {
          if (insertionIndex >= edgeList.length) {
            int newSize = edgeList.length;
            while (newSize <= insertionIndex) {
              if (newSize < 100) {
                newSize *= 2;
              } else {
                newSize += 100;
              }
            }
            edgeList = Arrays.copyOf(edgeList, newSize);
          }
          edgeList[insertionIndex] = edgeId;
          return edgeList;
        }

        private void clear() {
          vertexId2Index = new HashMap<>();
          for (int v = 0; v < vertexDegrees.size(); v++) {
            long[] vertex = vertexDegrees.get(v);
            for (int i = 0; i < vertex.length; i++) {
              vertex[i] = 0;
            }
            long[][] incidentEdge = incidentEdges.get(v);
            for (int i = 0; i < incidentEdge.length; i++) {
              for (int j = 0; j < incidentEdge[i].length; j++) {
                incidentEdge[i][j] = 0;
              }
            }
          }
        }

        @Override
        public boolean hasNextChunk() {
          return !vertexId2Index.isEmpty();
        }

        @Override
        public void sort(Comparator<long[]> comparator) {
          // empty final entries
          for (int i = vertexId2Index.size(); i < vertexDegrees.size(); i++) {
            long[] vertexDegree = vertexDegrees.get(i);
            vertexDegree[1] = 0;
            vertexDegree[2] = 0;
          }
          Collections.sort(vertexDegrees, comparator);
        }

        @Override
        public void writeChunk(LongOutputWriter output) throws IOException {
          for (long[] vertexDegree : vertexDegrees) {
            if ((vertexDegree[1] == 0) && (vertexDegree[2] == 0)) {
              continue;
            }
            output.writeLong(vertexDegree[0]);
            output.writeLong(vertexDegree[1]);
            output.writeLong(vertexDegree[2]);
            for (long outEdge : incidentEdges.get((int) vertexDegree[3])[0]) {
              if (outEdge == 0) {
                break;
              }
              output.writeLong(outEdge);
            }
            for (long inEdge : incidentEdges.get((int) vertexDegree[3])[1]) {
              if (inEdge == 0) {
                break;
              }
              output.writeLong(inEdge);
            }
          }
        }

        // TODO remove
        // @Override
        // public String toString() {
        // StringBuilder sb = new
        // StringBuilder("======================").append("\n");
        // String bigDelim = "";
        // for (long[] vertex : vertexDegrees) {
        // if ((vertex[1] == 0) && (vertex[2] == 0)) {
        // continue;
        // }
        // sb.append(bigDelim).append("vertex:").append(vertex[0]).append("\n");
        // sb.append("\toutdegree:").append(vertex[1]).append(" {");
        // String delim = "";
        // for (long edge : incidentEdges.get((int) vertex[3])[0]) {
        // if (edge == 0) {
        // break;
        // }
        // sb.append(delim).append(edge);
        // delim = ", ";
        // }
        // sb.append("}\n");
        // sb.append("\tindegree:").append(vertex[2]).append(" {");
        // delim = "";
        // for (long edge : incidentEdges.get((int) vertex[3])[1]) {
        // if (edge == 0) {
        // break;
        // }
        // sb.append(delim).append(edge);
        // delim = ", ";
        // }
        // sb.append("}");
        // bigDelim = "\n";
        // }
        // return sb.toString();
        // }

        @Override
        public void close() {
          numberOfEdges = nextEdgeId;
          vertexId2Index = null;
          vertexDegrees = null;
          incidentEdges = null;
        }
      };

      Merger merger = new Merger() {

        @Override
        public long[] readNextElement(LongIterator iterator) {
          return new long[] { iterator.next() };
        }

        @Override
        public void mergeAndWrite(BitSet indicesOfSmallestElement, long[][] elements,
                LongIterator[] iterators, LongOutputWriter out) throws IOException {
          // write vertexId
          out.writeLong(elements[indicesOfSmallestElement.nextSetBit(0)][0]);
          // collect out and in degrees
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
          out.writeLong(newOutDegree);
          out.writeLong(newInDegree);
          // merge out-going edge lists
          for (int i = indicesOfSmallestElement.nextSetBit(
                  0), k = 0; i >= 0; i = indicesOfSmallestElement.nextSetBit(i + 1), k++) {
            for (int j = 0; j < outDegrees[k]; j++) {
              out.writeLong(iterators[i].next());
            }
          }
          // merge in-going edge lists
          for (int i = indicesOfSmallestElement.nextSetBit(
                  0), k = 0; i >= 0; i = indicesOfSmallestElement.nextSetBit(i + 1), k++) {
            for (int j = 0; j < inDegrees[k]; j++) {
              out.writeLong(iterators[i].next());
            }
          }
        }
      };

      Comparator<long[]> comparator = new FixedSizeLongArrayComparator(true, 0);

      File vertexEdgeListFile = File.createTempFile("vertexEdgeListFile", "", workingDir);
      try (EncodedLongFileOutputStream output = new EncodedLongFileOutputStream(
              vertexEdgeListFile);) {
        NWayMergeSort mergeSort = new NWayMergeSort();
        mergeSort.sort(producer, merger, comparator, workingDir, maxNumberOfOpenFiles, output);
      }
      return vertexEdgeListFile;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (producer != null) {
        producer.close();
      }
    }
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

  private void printSimple(File vertexIncidentEdgesFile) {
    try (EncodedLongFileInputStream in = new EncodedLongFileInputStream(vertexIncidentEdgesFile);) {
      LongIterator iterator = in.iterator();
      while (iterator.hasNext()) {
        System.out.println("vertex" + iterator.next() + " out:" + iterator.next() + " in:"
                + iterator.next() + " offset:" + iterator.next());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
