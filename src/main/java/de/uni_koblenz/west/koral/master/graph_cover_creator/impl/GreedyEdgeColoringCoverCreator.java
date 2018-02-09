package de.uni_koblenz.west.koral.master.graph_cover_creator.impl;

import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedFileOutputStream;
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
import de.uni_koblenz.west.koral.master.utils.VertexDegreeComparator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
    long sortStart = System.currentTimeMillis();

    // sort by degree
    File[] verticesByDegreeLevels = splitIntoDegreeLevels(vertexIncidentEdgesFile,
            internalWorkingDir, GreedyEdgeColoringCoverCreator.NUMBER_OF_CACHED_VERTICES,
            GreedyEdgeColoringCoverCreator.MAX_NUMBER_OF_OPEN_FILES);
    // TODO remove
    System.out.println("spliting by degree time: " + (System.currentTimeMillis() - sortStart));
    sortStart = System.currentTimeMillis();
    File sortedVertexList = null;
    for (int level = 0; level < verticesByDegreeLevels.length; level++) {
      sortedVertexList = sortByDegree(level, verticesByDegreeLevels[level], sortedVertexList,
              internalWorkingDir, GreedyEdgeColoringCoverCreator.NUMBER_OF_CACHED_VERTICES,
              GreedyEdgeColoringCoverCreator.NUMBER_OF_CACHED_EDGES,
              GreedyEdgeColoringCoverCreator.MAX_NUMBER_OF_OPEN_FILES);
    }
    // TODO remove
    System.out.println("sort by degree time: " + (System.currentTimeMillis() - sortStart));
    // TODO remove
    System.out.println(">>>>total number of edges: " + numberOfEdges);
    // TODO remove
    fillEdgeInformation(sortedVertexList);

    File edges2chunks = null;
    try (ColoringManager colorManager = new ColoringManager(internalWorkingDir,
            GreedyEdgeColoringCoverCreator.MAX_NUMBER_OF_OPEN_FILES / 2);) {
      // create edge coloring
      // TODO remove
      long coloringStart = System.currentTimeMillis();
      createEdgeColoring(sortedVertexList, colorManager, internalWorkingDir, numberOfEdges,
              numberOfGraphChunks, GreedyEdgeColoringCoverCreator.NUMBER_OF_CACHED_EDGES,
              GreedyEdgeColoringCoverCreator.MAX_NUMBER_OF_OPEN_FILES);
      // TODO remove
      System.out.println(
              "creation of edge coloring: " + (System.currentTimeMillis() - coloringStart));
      // TODO remove
      // createGraphChunkPerColor(colorManager, input, workingDir);
      // assign edges to graph chunks
      // TODO remove
      long edgeAssignmentStart = System.currentTimeMillis();
      edges2chunks = createAssignmentOfEdgesToChunks(colorManager, internalWorkingDir,
              numberOfEdges, numberOfGraphChunks,
              GreedyEdgeColoringCoverCreator.NUMBER_OF_CACHED_EDGES,
              GreedyEdgeColoringCoverCreator.NUMBER_OF_CACHED_VERTICES,
              GreedyEdgeColoringCoverCreator.MAX_NUMBER_OF_OPEN_FILES / 2);
      // TODO remove
      System.out.println(
              "assigning edges to chunks: " + (System.currentTimeMillis() - edgeAssignmentStart));
    }

    // sort edges2chunks by edgeIds in ascending order
    // output is a list of partitionIds: e1->4;e2->2 is stored as [4,2]
    // TODO remove
    long createAssignmentFileStart = System.currentTimeMillis();
    File edgeAssignment = null;
    try (EncodedLongFileInputStream edges2ChunksInput = new EncodedLongFileInputStream(
            edges2chunks);) {
      edgeAssignment = sortBinaryValues(edges2ChunksInput,
              new FixedSizeLongArrayComparator(true, 0), internalWorkingDir,
              GreedyEdgeColoringCoverCreator.NUMBER_OF_CACHED_VERTICES,
              GreedyEdgeColoringCoverCreator.MAX_NUMBER_OF_OPEN_FILES, true);
      edges2chunks.delete();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    // TODO remove
    System.out.println("create sequence of chunk ids: "
            + (System.currentTimeMillis() - createAssignmentFileStart));

    // iterate partitionIds and input in parallel to translate edgeId back into
    // triple
    // TODO remove
    long writeChunksStart = System.currentTimeMillis();
    try (EncodedFileInputStream newInput = new EncodedFileInputStream(input);
            EncodedLongFileInputStream chunkInput = new EncodedLongFileInputStream(
                    edgeAssignment);) {
      Iterator<Statement> tripleIterator = newInput.iterator();
      LongIterator chunkIterator = chunkInput.iterator();
      while (tripleIterator.hasNext() && chunkIterator.hasNext()) {
        Statement stmt = tripleIterator.next();
        long chunkIndex = chunkIterator.next();
        writeStatementToChunk((int) chunkIndex, numberOfGraphChunks, stmt, outputs, writtenFiles);
      }
      if (tripleIterator.hasNext()) {
        throw new RuntimeException("There exist triples that were not assigned to a chunk.");
      }
      if (chunkIterator.hasNext()) {
        throw new RuntimeException("There exist more chunk assignments than triples.");
      }
      // TODO Auto-generated method stub
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    // TODO remove
    System.out.println("writing chunks: " + (System.currentTimeMillis() - writeChunksStart));

    deleteFolder(internalWorkingDir);
    // TODO remove
    long requiredTime = System.currentTimeMillis() - start;
    System.out.println("required time: " + requiredTime);
  }

  private File createAssignmentOfEdgesToChunks(ColoringManager colorManager, File workingDir,
          long numberOfEdges, int numberOfGraphChunks, int numberOfCachedEdges,
          int numberOfCachedVertices, int maxNumberOfOpenFiles) {
    try {
      // TODO remove
      long sortColorsStart = System.currentTimeMillis();
      // sort colors by size in descending order
      Iterator<long[]> iteratorOverColors = colorManager.getIteratorOverAllColors();
      File colorsSortedBySizeDesc = sortBinaryValues(iteratorOverColors,
              new FixedSizeLongArrayComparator(false, 1, 0), workingDir, numberOfCachedVertices,
              maxNumberOfOpenFiles, false);
      // TODO remove
      System.out.println(
              "\tsorting colors by size: " + (System.currentTimeMillis() - sortColorsStart));

      // perform greedy algorithm to assign colors to chunk
      // TODO remove
      long assigningColors2ChunksStart = System.currentTimeMillis();
      long[] chunkSizes = new long[numberOfGraphChunks];
      File color2chunks = File.createTempFile("colors2chunks", "", workingDir);
      try (EncodedLongFileInputStream input = new EncodedLongFileInputStream(
              colorsSortedBySizeDesc);
              LongIterator iterator = input.iterator();
              EncodedLongFileOutputStream output = new EncodedLongFileOutputStream(color2chunks);) {
        while (iterator.hasNext()) {
          long colorId = iterator.next();
          long colorSize = iterator.next();
          // find chunk with minimal size
          int indexOfMinChunk = 0;
          for (int i = 1; i < chunkSizes.length; i++) {
            if (chunkSizes[i] < chunkSizes[indexOfMinChunk]) {
              indexOfMinChunk = i;
            }
          }
          // add color to chunk
          chunkSizes[indexOfMinChunk] += colorSize;
          output.writeLong(colorId);
          output.writeLong(indexOfMinChunk);
        }
      }
      colorsSortedBySizeDesc.delete();
      // TODO remove
      System.out.println("\tassigning colors to chunks: "
              + (System.currentTimeMillis() - assigningColors2ChunksStart));

      // TODO remove
      long sortingChunkAssignmentByColorStart = System.currentTimeMillis();
      // sort color2chunks by colorIds
      File color2chunksSortedByColorAsc = null;
      try (EncodedLongFileInputStream input = new EncodedLongFileInputStream(color2chunks);) {
        color2chunksSortedByColorAsc = sortBinaryValues(input,
                new FixedSizeLongArrayComparator(true, 0), workingDir, numberOfCachedVertices,
                maxNumberOfOpenFiles, false);
      }
      color2chunks.delete();
      // TODO remove
      System.out.println("\tsorting color2chunks by colorId: "
              + (System.currentTimeMillis() - sortingChunkAssignmentByColorStart));

      // TODO remove
      long sortEdge2colorStart = System.currentTimeMillis();
      // get assignment of edges to colors sorted by colors in ascending order
      Iterator<long[]> iteratorOverColoredEdges = colorManager.getIteratorOverColoredEdges();
      File edges2ColorsSortedByColorAsc = sortBinaryValues(iteratorOverColoredEdges,
              new FixedSizeLongArrayComparator(true, 1, 0), workingDir, numberOfCachedVertices,
              maxNumberOfOpenFiles, false);
      // TODO remove
      System.out.println("\tsorting edge2color by colorId: "
              + (System.currentTimeMillis() - sortEdge2colorStart));

      // TODO remove
      long joinStart = System.currentTimeMillis();
      // join edges2color colors2chunks -> edges2chunks
      File edges2chunks = File.createTempFile("edges2chunks-", "", workingDir);
      try (EncodedLongFileOutputStream output = new EncodedLongFileOutputStream(edges2chunks);
              EncodedLongFileInputStream colors2chunksInput = new EncodedLongFileInputStream(
                      color2chunksSortedByColorAsc);
              LongIterator colors2chunksIterator = colors2chunksInput.iterator();
              EncodedLongFileInputStream edges2colorsInput = new EncodedLongFileInputStream(
                      edges2ColorsSortedByColorAsc);
              LongIterator edges2colorsIterator = edges2colorsInput.iterator();) {
        long colorId = 0;
        long chunkId = -1;
        long edgeId = 0;
        long edgeColor = 0;
        while (colors2chunksIterator.hasNext()) {
          colorId = colors2chunksIterator.next();
          chunkId = colors2chunksIterator.next();
          if (edgeId == 0) {
            edgeId = edges2colorsIterator.next();
            edgeColor = edges2colorsIterator.next();
          }
          if (edgeColor != colorId) {
            throw new RuntimeException("edge e" + edgeId + " has color c" + edgeColor
                    + " but should have color c" + colorId + ".");
          }
          do {
            output.writeLong(edgeId);
            output.writeLong(chunkId);
            if (edges2colorsIterator.hasNext()) {
              edgeId = edges2colorsIterator.next();
              edgeColor = edges2colorsIterator.next();
            } else {
              edgeColor = 0;
            }
          } while (edgeColor == colorId);
        }
        if (edges2colorsIterator.hasNext()) {
          throw new RuntimeException("There exist edges with unknown colors.");
        }
      }
      color2chunksSortedByColorAsc.delete();
      edges2ColorsSortedByColorAsc.delete();
      // TODO remove
      System.out.println("\tjoining edge2color and color2chunkId: "
              + (System.currentTimeMillis() - joinStart));
      return edges2chunks;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private File sortBinaryValues(EncodedLongFileInputStream input, Comparator<long[]> comparator,
          File workingDir, int numberOfCachedVertices, int maxNumberOfOpenFiles,
          boolean outputOnlySecondValue) {
    Iterator<long[]> iterator = new Iterator<long[]>() {

      private final LongIterator iter = input.iterator();

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public long[] next() {
        return new long[] { iter.next(), iter.next() };
      }
    };
    return sortBinaryValues(iterator, comparator, workingDir, numberOfCachedVertices,
            maxNumberOfOpenFiles, outputOnlySecondValue);
  }

  private File sortBinaryValues(Iterator<long[]> iterator, Comparator<long[]> comparator,
          File workingDir, int numberOfCachedVertices, int maxNumberOfOpenFiles,
          boolean outputOnlySecondValue) {
    try {
      File sortedValuesFile = File.createTempFile("sortedValues-", "", workingDir);
      try (EncodedLongFileOutputStream output = new EncodedLongFileOutputStream(sortedValuesFile);
              InitialChunkProducer producer = new InitialChunkProducer() {

                private long[][] values;

                private int nextIndex;

                @Override
                public void loadNextChunk() throws IOException {
                  if (values == null) {
                    values = new long[numberOfCachedVertices][];
                  }
                  nextIndex = 0;
                  while (iterator.hasNext() && (nextIndex < values.length)) {
                    values[nextIndex] = iterator.next();
                    nextIndex++;
                  }
                }

                @Override
                public boolean hasNextChunk() {
                  return nextIndex > 0;
                }

                @Override
                public void sort(Comparator<long[]> comparator) {
                  Arrays.sort(values, 0, nextIndex, comparator);
                }

                @Override
                public void writeChunk(LongOutputWriter output) throws IOException {
                  for (int i = 0; i < nextIndex; i++) {
                    long[] color = values[i];
                    for (long value : color) {
                      output.writeLong(value);
                    }
                  }
                }

                @Override
                public void close() {
                  values = null;
                }
              };) {
        Merger merger = new Merger() {

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
            for (int index = indicesOfSmallestElement.nextSetBit(
                    0); index >= 0; index = indicesOfSmallestElement.nextSetBit(index + 1)) {
              if (outputOnlySecondValue && (out == output)) {
                out.writeLong(elements[index][1]);
              } else {
                for (long value : elements[index]) {
                  out.writeLong(value);
                }
              }
            }
          }

          @Override
          public void close() throws Exception {
          }
        };
        NWayMergeSort sort = new NWayMergeSort();
        sort.sort(producer, merger, comparator, workingDir, maxNumberOfOpenFiles - 2, output);
      }
      return sortedValuesFile;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // TODO remove
  private long processedEdges;

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
        processedEdges = 0;
        // TODO remove
        // System.out.println("v" + vertexId + " degree:" + degree);
        if (degree <= maxInMemoryEdgeNumber) {
          if ((edges == null) || (degree > edges.length)) {
            edges = new long[(int) degree][3];
          }
          for (int i = 0; i < degree; i++) {
            edges[i][0] = iterator.next();
          }
          // TODO remove
          // colorManager.fillColorInformation(edges);
          Arrays.sort(edges, new FixedSizeLongArrayComparator(false, 2, 1, 0));
          // TODO remove
          // if (vertexId == 16) {
          // System.out.println(Arrays.deepToString(edges));
          // }
          try (EdgeIterator iter = new EdgeArrayIterator(edges);) {
            colourEdges(iter, colorManager, maxNumberOfEdgesPerColour);
          }
          // TODO remove
          if (edges.length != processedEdges) {
            System.out.println(
                    "in memory: original=" + edges.length + " processed:" + processedEdges);
          }
          // if (vertexId == 16) {
          // System.out.println(Arrays.deepToString(edges));
          // }
        } else {
          edges = null;
          File edgeColors = getEdgeColors(iterator, outDegree, inDegree, colorManager, workingDir,
                  (int) maxInMemoryEdgeNumber, (maxNumberOfOpenFiles / 2) - 1);
          try (EdgeIterator iter = new EdgeFileIterator(edgeColors);) {
            colourEdges(iter, colorManager, maxNumberOfEdgesPerColour);
          }
          // TODO remove
          long orignal = countEntries(edgeColors, 3);
          if (orignal != processedEdges) {
            System.out.println("in memory: original=" + orignal + " processed:" + processedEdges);
          }
          edgeColors.delete();
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    // TODO remove
    System.out.println(">>>>number of uncolored edges: " + numberOfUncoloredEdges);
    // System.out.println(">>>>remaining edges: " + edge2vertex.size());
    // print(edge2vertex, new File(workingDir.getAbsolutePath() + File.separator
    // + "edge2vertex.txt"));
  }

  private File getEdgeColors(LongIterator iterator, long outDegree, long inDegree,
          ColoringManager colorManager, File workingDir, int maxInMemoryEdgeNumber,
          int maxNumberOfOpenFiles) throws IOException {
    try (InitialChunkProducer producer = new InitialChunkProducer() {

      private long remainingNumberOfEdgesToRead;

      private long[][] cachedEdges;

      private int nextIndex;

      @Override
      public void loadNextChunk() throws IOException {
        if (cachedEdges == null) {
          cachedEdges = new long[maxInMemoryEdgeNumber][3];
          remainingNumberOfEdgesToRead = outDegree + inDegree;
        }
        nextIndex = 0;
        while ((remainingNumberOfEdgesToRead > 0) && (nextIndex < cachedEdges.length)) {
          cachedEdges[nextIndex][0] = iterator.next();
          nextIndex++;
          remainingNumberOfEdgesToRead--;
        }
        colorManager.fillColorInformation(cachedEdges, nextIndex);
      }

      @Override
      public boolean hasNextChunk() {
        return nextIndex > 0;
      }

      @Override
      public void sort(Comparator<long[]> comparator) {
        Arrays.sort(cachedEdges, 0, nextIndex, comparator);
      }

      @Override
      public void writeChunk(LongOutputWriter output) throws IOException {
        long previousEdge = 0;
        long previousColor = 0;
        for (int j = 0; j < nextIndex; j++) {
          if ((cachedEdges[j][1] == 0) || (cachedEdges[j][1] != previousColor)
                  || (cachedEdges[j][0] == previousEdge)) {
            output.writeLong(cachedEdges[j][0]);
            output.writeLong(cachedEdges[j][1]);
            output.writeLong(cachedEdges[j][2]);
            previousEdge = cachedEdges[j][0];
            previousColor = cachedEdges[j][1];
          }
        }
      }

      @Override
      public void close() {
        cachedEdges = null;
      }
    }) {

      Merger merger = new Merger() {

        private long[] previousEdge;

        @Override
        public void startNextMergeLevel() {
          previousEdge = new long[3];
        }

        @Override
        public long[] readNextElement(LongIterator iterator) throws IOException {
          return new long[] { iterator.next(), iterator.next(), iterator.next() };
        }

        @Override
        public void mergeAndWrite(BitSet indicesOfSmallestElement, long[][] elements,
                LongIterator[] iterators, LongOutputWriter out) throws IOException {
          for (int i = indicesOfSmallestElement.nextSetBit(0); i >= 0; i = indicesOfSmallestElement
                  .nextSetBit(i + 1)) {
            long[] edge = elements[i];
            if (edge[1] == 0) {
              // this is an uncolored edge
              if ((previousEdge[0] == 0) || (previousEdge[0] != edge[0])) {
                // if it is not a self-loop write it
                out.writeLong(edge[0]);
                out.writeLong(edge[1]);
                out.writeLong(edge[2]);
              }
            } else {
              // this is a colored edge
              if ((previousEdge[0] == 0) || (previousEdge[1] != edge[1])) {
                // if it is a new color, write it
                out.writeLong(edge[0]);
                out.writeLong(edge[1]);
                out.writeLong(edge[2]);
              }
            }
            previousEdge[0] = edge[0];
            previousEdge[1] = edge[1];
            previousEdge[2] = edge[2];
          }
        }

        @Override
        public void close() throws Exception {
        }
      };

      Comparator<long[]> comparator = new FixedSizeLongArrayComparator(false, 2, 1, 0);

      File edgeColorsFile = File.createTempFile("edgeColors-", "", workingDir);
      try (EncodedLongFileOutputStream output = new EncodedLongFileOutputStream(edgeColorsFile);) {
        NWayMergeSort mergeSort = new NWayMergeSort();
        mergeSort.sort(producer, merger, comparator, workingDir, maxNumberOfOpenFiles - 2, output);
      }
      return edgeColorsFile;
    }
  }

  // TODO remove
  private long numberOfUncoloredEdges = 0;
  private final Map<Long, long[]> edge2vertex = new HashMap<>();

  private void colourEdges(EdgeIterator edges, ColoringManager colorManager,
          long maxNumberOfEdgesPerColour) {
    /*
     * color[0] = colorId; color[1] = frequency
     */
    List<long[]> colorCache = new LinkedList<>();
    Map<Long, long[]> recoloring = new HashMap<>();
    // TODO check number of cached colors
    long[] edge = null;
    // iterate over all colored edges
    while (((edge == null) || (edge[1] != 0)) && edges.hasNext()) {
      // read next edge
      edge = edges.next();
      processedEdges++;
      if (edge[1] == 0) {
        break;
      }
      updateColor(recoloring, edge);
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
              addRecoloringEntry(recoloring, joinedColor, edge[1]);
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
    }
    recoloring = null;
    // sort colors by frequency
    Collections.sort(colorCache, new FixedSizeLongArrayComparator(false, 1));
    // iterate over all uncolored edges
    long[] previousEdge = null;
    if ((edge != null) && (edge[1] == 0)) {
      numberOfUncoloredEdges++;
      edge2vertex.remove(edge[0]);
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
      while (edges.hasNext()) {
        // read next edge
        previousEdge = edge;
        edge = edges.next();
        processedEdges++;
        if (previousEdge[0] == edge[0]) {
          // this is a self-loop
          continue;
        }
        numberOfUncoloredEdges++;
        edge2vertex.remove(edge[0]);
        color = null;
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
      }
    }
  }

  private void updateColor(Map<Long, long[]> recoloring, long[] edge) {
    long[] newColor = getColorInformation(recoloring, edge[1]);
    if (newColor != null) {
      edge[1] = newColor[0];
      edge[2] = newColor[1];
    }
  }

  private long[] getColorInformation(Map<Long, long[]> recoloring, long oldColor) {
    long[] replacedColor = recoloring.get(oldColor);
    if (replacedColor == null) {
      return null;
    }
    Set<Long> replacedColors = new HashSet<>();
    while (replacedColor.length == 1) {
      replacedColors.add(oldColor);
      oldColor = replacedColor[0];
      replacedColor = recoloring.get(replacedColor[0]);
    }
    for (long color : replacedColors) {
      // recolored colors were recolored
      // shorten the search path
      recoloring.put(color, replacedColor);
    }
    return replacedColor;
  }

  private void addRecoloringEntry(Map<Long, long[]> recoloring, long[] joinedColor, long oldColor) {
    recoloring.put(oldColor, joinedColor);
  }

  private File sortByDegree(int level, File vertexFile, File outputFile, File workingDir,
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
        sortMixedLevelByDegree(vertexFile, outputFile, workingDir, maxNumberOfOpenFiles,
                numberOfCachedVertices, numberOfCachedEdges, maxDegree);
        vertexFile.delete();
        return outputFile;
      }
    }
  }

  private void sortMixedLevelByDegree(File vertexFile, File outputFile, File workingDir,
          int maxNumberOfOpenFiles, int numberOfCachedVertices, int numberOfCachedEdges,
          long maxDegree) {
    final int numberOfCachedVerticesPerLevel = (int) ((numberOfCachedEdges / maxDegree) > 65536
            ? 65536
            : numberOfCachedEdges / maxDegree);
    InitialChunkProducer producer = null;
    try (final EncodedLongFileOutputStream output = new EncodedLongFileOutputStream(outputFile,
            true);) {
      final File edgeListFile = File.createTempFile("edgeListFile-", "", workingDir);
      producer = new InitialChunkProducer() {

        private EncodedLongFileInputStream inputFile = null;

        private LongIterator iterator = null;

        private long[][] vertexHeaders = new long[numberOfCachedVerticesPerLevel > 0
                ? numberOfCachedVerticesPerLevel
                : numberOfCachedVertices][4];

        private long[][] outEdges = new long[numberOfCachedVerticesPerLevel][];

        private long[][] inEdges = new long[numberOfCachedVerticesPerLevel][];

        private EncodedRandomAccessLongFileOutputStream output;

        private int nextIndex;

        @Override
        public void loadNextChunk() throws IOException {
          if (inputFile == null) {
            inputFile = new EncodedLongFileInputStream(vertexFile);
            iterator = inputFile.iterator();
            if (numberOfCachedVerticesPerLevel == 0) {
              // there are too many edges to store them in memory
              output = new EncodedRandomAccessLongFileOutputStream(edgeListFile);
            }
          }
          nextIndex = 0;
          while ((nextIndex < vertexHeaders.length) && iterator.hasNext()) {
            vertexHeaders[nextIndex][0] = iterator.next();
            vertexHeaders[nextIndex][1] = iterator.next();
            vertexHeaders[nextIndex][2] = iterator.next();
            if (output == null) {
              vertexHeaders[nextIndex][3] = nextIndex;
              outEdges[nextIndex] = readEdges(outEdges[nextIndex], iterator,
                      (int) vertexHeaders[nextIndex][1]);
              inEdges[nextIndex] = readEdges(inEdges[nextIndex], iterator,
                      (int) vertexHeaders[nextIndex][2]);
            } else {
              // store edges in a separate file
              vertexHeaders[nextIndex][3] = output.getPosition();
              for (int i = 0; i < vertexHeaders[nextIndex][1]; i++) {
                output.writeLong(iterator.next());
              }
              for (int i = 0; i < vertexHeaders[nextIndex][2]; i++) {
                output.writeLong(iterator.next());
              }
            }
            nextIndex++;
          }
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

        @Override
        public boolean hasNextChunk() {
          return nextIndex > 0;
        }

        @Override
        public void sort(Comparator<long[]> comparator) {
          Arrays.sort(vertexHeaders, 0, nextIndex, comparator);
        }

        @Override
        public void writeChunk(LongOutputWriter output) throws IOException {
          for (int i = 0; i < nextIndex; i++) {
            output.writeLong(vertexHeaders[i][0]);
            output.writeLong(vertexHeaders[i][1]);
            output.writeLong(vertexHeaders[i][2]);
            if (this.output == null) {
              int edgeIndex = (int) vertexHeaders[i][3];
              for (int j = 0; j < vertexHeaders[i][1]; j++) {
                output.writeLong(outEdges[edgeIndex][j]);
              }
              for (int j = 0; j < vertexHeaders[i][2]; j++) {
                output.writeLong(inEdges[edgeIndex][j]);
              }
            } else {
              // the edges are stored in a file
              output.writeLong(vertexHeaders[i][3]);
            }
          }
        }

        @Override
        public void close() {
          vertexHeaders = null;
          outEdges = null;
          inEdges = null;
          if (iterator != null) {
            iterator.close();
          }
          if (inputFile != null) {
            try {
              inputFile.close();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
          if (output != null) {
            try {
              output.close();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      };

      Merger merger = new Merger() {

        private EncodedRandomAccessLongFileInputStream edgeListInput;

        @Override
        public void startNextMergeLevel() {
        }

        @Override
        public long[] readNextElement(LongIterator iterator) {
          if (numberOfCachedVerticesPerLevel > 0) {
            return new long[] { iterator.next(), iterator.next(), iterator.next() };
          } else {
            // read the additional edge offset of the edge file
            return new long[] { iterator.next(), iterator.next(), iterator.next(),
                    iterator.next() };
          }
        }

        @Override
        public void mergeAndWrite(BitSet indicesOfSmallestElement, long[][] elements,
                LongIterator[] iterators, LongOutputWriter out) throws IOException {
          for (int index = indicesOfSmallestElement.nextSetBit(
                  0); index >= 0; index = indicesOfSmallestElement.nextSetBit(index + 1)) {
            out.writeLong(elements[index][0]);
            out.writeLong(elements[index][1]);
            out.writeLong(elements[index][2]);
            if (numberOfCachedVerticesPerLevel > 0) {
              // copy edge lists
              for (int i = 0; i < elements[index][1]; i++) {
                out.writeLong(iterators[index].next());
              }
              for (int i = 0; i < elements[index][2]; i++) {
                out.writeLong(iterators[index].next());
              }
            } else {
              if (output != out) {
                // this is in intermediate merge step
                out.writeLong(elements[index][3]);
              } else {
                // this is the final output
                // also add the edges from the separate edge list
                if (edgeListInput == null) {
                  edgeListInput = new EncodedRandomAccessLongFileInputStream(edgeListFile);
                }
                edgeListInput.setPosition(elements[index][3]);
                for (int i = 0; i < elements[index][1]; i++) {
                  out.writeLong(edgeListInput.readLong());
                }
                for (int i = 0; i < elements[index][2]; i++) {
                  out.writeLong(edgeListInput.readLong());
                }
              }
            }
          }
        }

        @Override
        public void close() throws Exception {
          if (edgeListInput != null) {
            edgeListInput.close();
          }
        }
      };

      Comparator<long[]> comparator = VertexDegreeComparator.getComparator(true);

      NWayMergeSort mergesort = new NWayMergeSort();
      mergesort.sort(producer, merger, comparator, workingDir, maxNumberOfOpenFiles - 3, output);
      edgeListFile.delete();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (producer != null) {
        producer.close();
      }
    }
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

        @Override
        public void close() {
          numberOfEdges = nextEdgeId - 1;
          vertexId2Index = null;
          vertexDegrees = null;
          incidentEdges = null;
        }
      };

      Merger merger = new Merger() {

        @Override
        public void startNextMergeLevel() {
        }

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

        @Override
        public void close() throws Exception {
        }
      };

      Comparator<long[]> comparator = new FixedSizeLongArrayComparator(true, 0);

      File vertexEdgeListFile = File.createTempFile("vertexEdgeListFile", "", workingDir);
      try (EncodedLongFileOutputStream output = new EncodedLongFileOutputStream(
              vertexEdgeListFile);) {
        NWayMergeSort mergeSort = new NWayMergeSort();
        mergeSort.sort(producer, merger, comparator, workingDir, maxNumberOfOpenFiles - 2, output);
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

  private long countEntries(File input, int numberOfValuesPerRow) {
    long countedValues = 0;
    try (EncodedLongFileInputStream in = new EncodedLongFileInputStream(input);) {
      LongIterator iterator = in.iterator();
      while (iterator.hasNext()) {
        countedValues++;
        for (int i = 0; i < numberOfValuesPerRow; i++) {
          iterator.next();
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return countedValues;
  }

  private void fillEdgeInformation(File input) {
    try (EncodedLongFileInputStream in = new EncodedLongFileInputStream(input);) {
      LongIterator iterator = in.iterator();
      while (iterator.hasNext()) {
        long vertex = iterator.next();
        long outDegree = iterator.next();
        long inDegree = iterator.next();
        for (int i = 0; i < outDegree; i++) {
          edge2vertex.put(iterator.next(), new long[] { vertex, outDegree + inDegree });
        }
        for (int i = 0; i < inDegree; i++) {
          iterator.next();
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void print(Map<Long, long[]> edge2vertex, File file) {
    try (BufferedWriter out = new BufferedWriter(new FileWriter(file));) {
      for (Entry<Long, long[]> entry : edge2vertex.entrySet()) {
        String line = "e" + entry.getKey().longValue() + " v" + entry.getValue()[0] + " degree:"
                + entry.getValue()[1] + "\n";
        out.write(line);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void printBinary(File input, int numberOfNumbers) {
    try (EncodedLongFileInputStream in = new EncodedLongFileInputStream(input);) {
      LongIterator iterator = in.iterator();
      while (iterator.hasNext()) {
        System.out.print("[");
        for (int i = 0; i < numberOfNumbers; i++) {
          System.out.print((i == 0 ? "" : ", ") + iterator.next());
        }
        System.out.println("]");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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

  private void createGraphChunkPerColor(ColoringManager colorManager, EncodedFileInputStream input,
          File workingDir) {
    try (EncodedFileInputStream newInput = new EncodedFileInputStream(input);) {
      File colorDir = new File(workingDir.getCanonicalPath() + File.separator + "coloring");
      if (colorDir.exists()) {
        deleteFolder(colorDir);
      }
      colorDir.mkdirs();

      File sortedColors = sortBinaryValues(colorManager.getIteratorOverColoredEdges(),
              new FixedSizeLongArrayComparator(true, 0), workingDir,
              GreedyEdgeColoringCoverCreator.NUMBER_OF_CACHED_VERTICES, 100, true);

      Map<Long, EncodedFileOutputStream> outputs = new HashMap<>();
      try (EncodedLongFileInputStream sortedColorsInput = new EncodedLongFileInputStream(
              sortedColors);) {
        for (Statement stmt : newInput) {
          long color = sortedColorsInput.readLong();
          EncodedFileOutputStream output = outputs.get(color);
          if (output == null) {
            output = new EncodedFileOutputStream(new File(colorDir.getCanonicalFile()
                    + File.separator + "chunkColor" + color + ".enc.gz"));
            outputs.put(color, output);
          }
          output.writeStatement(stmt);
        }
      }
      sortedColors.delete();
      for (EncodedFileOutputStream output : outputs.values()) {
        try {
          output.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
