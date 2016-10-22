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

import org.apache.jena.iri.IRI;
import org.apache.jena.riot.system.IRIResolver;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedFileOutputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.measurement.MeasurementType;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Creates a hierarchical hash cover.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class HierarchicalCoverCreator extends HashCoverCreator {

  public HierarchicalCoverCreator(Logger logger, MeasurementCollector measurementCollector) {
    super(logger, measurementCollector);
  }

  @Override
  public EncodingFileFormat getRequiredInputEncoding() {
    return EncodingFileFormat.UEE;
  }

  @Override
  protected void createCover(DictionaryEncoder dictionary, EncodedFileInputStream input,
          int numberOfGraphChunks, EncodedFileOutputStream[] outputs, boolean[] writtenFiles,
          File workingDir) {
    int hierarchyLevel = identifyHierarchyLevel(input, numberOfGraphChunks);
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_COVER_CREATION_FILE_WRITE_START,
              System.currentTimeMillis());
    }
    try {
      input.close();
      input = new EncodedFileInputStream(input);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    for (Statement statement : input) {
      String subjectString = statement.getSubjectAsString();
      if (!isUri(subjectString)) {
        processStatement(numberOfGraphChunks, outputs, writtenFiles, statement);
      } else {
        String[] iriParts = getIRIHierarchy(subjectString);
        String iriPrefix = getIriPrefix(iriParts, hierarchyLevel);
        int targetChunk = computeHash(iriPrefix) % outputs.length;
        if (targetChunk < 0) {
          targetChunk *= -1;
        }

        writeStatementToChunk(targetChunk, numberOfGraphChunks, statement, outputs, writtenFiles);
      }
    }
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_COVER_CREATION_FILE_WRITE_END,
              System.currentTimeMillis());
    }
  }

  private boolean isUri(String subjectString) {
    if (subjectString.startsWith("<")) {
      subjectString = subjectString.substring(1);
      if (subjectString.endsWith(">")) {
        subjectString = subjectString.substring(0, subjectString.length() - 1);
      }
    }
    return !subjectString.startsWith(Configuration.BLANK_NODE_URI_PREFIX);
  }

  private int identifyHierarchyLevel(EncodedFileInputStream input, int numberOfGraphChunks) {
    if (measurementCollector != null) {
      measurementCollector.measureValue(
              MeasurementType.LOAD_GRAPH_COVER_CREATION_HIERARCHY_LEVEL_IDENTIFICATION_START,
              System.currentTimeMillis());
    }
    /*
     * The first dimension identifies the hierarchy level. Level 0 stores the
     * number of triples that have not an IRI as subject. They are not counted
     * in the following levels.
     * 
     * The second dimension aggregates the number of triples per graph chunk on
     * the given hierarchy. The length of this dimension is the number of graph
     * chunks.
     * 
     * The third dimension consists of tuples. The first element stores the
     * total number of triples assigned to a graph chunk at this hierarchy
     * level. The second element counts the number of triples assigned to this
     * chunk whose hierarchy level is <= the current level (i.e., they have too
     * short IRIs)
     */
    long[][][] tripleOccurences = new long[1][numberOfGraphChunks][2];
    tripleOccurences = computeTripleFrequencyPerHierarchyLevel(tripleOccurences, input,
            numberOfGraphChunks);

    int balancedHierarchyLevel = Integer.MAX_VALUE;
    double minBalance = Double.MAX_VALUE;
    for (int i = 1; i < tripleOccurences.length; i++) {
      double currentHierarchyLevelBalance = getHierarchyLevelBalance(tripleOccurences[0],
              tripleOccurences[i]);
      if (currentHierarchyLevelBalance < minBalance) {
        balancedHierarchyLevel = i;
        minBalance = currentHierarchyLevelBalance;
      }
    }

    if (measurementCollector != null) {
      measurementCollector.measureValue(
              MeasurementType.LOAD_GRAPH_COVER_CREATION_HIERARCHY_LEVEL_IDENTIFICATION_END,
              System.currentTimeMillis());
    }

    return balancedHierarchyLevel - 1;
  }

  private double getHierarchyLevelBalance(long[][] nonIriTriple, long[][] iriTriples) {
    double averageNumberOfTriples = 0;
    for (int chunk = 0; chunk < iriTriples.length; chunk++) {
      averageNumberOfTriples += nonIriTriple[chunk][0] + iriTriples[chunk][0];
    }
    averageNumberOfTriples /= iriTriples.length;

    double standardDeviation = 0;
    for (int chunk = 0; chunk < iriTriples.length; chunk++) {
      double factor = (nonIriTriple[chunk][0] + iriTriples[chunk][0]) - averageNumberOfTriples;
      standardDeviation += factor * factor;
    }
    standardDeviation /= iriTriples.length;
    return Math.sqrt(standardDeviation);
  }

  private long[][][] computeTripleFrequencyPerHierarchyLevel(long[][][] tripleOccurences,
          EncodedFileInputStream input, int numberOfGraphChunks) {
    for (Statement statement : input) {
      String subjectString = statement.getSubjectAsString();
      if (!isUri(subjectString)) {
        // assign to triple to chunk according to hash on subject
        int targetChunk = computeHash(subjectString) % numberOfGraphChunks;
        if (targetChunk < 0) {
          targetChunk *= -1;
        }
        tripleOccurences[0][targetChunk][0]++;
      } else {
        String[] iriParts = getIRIHierarchy(subjectString);
        if (iriParts.length >= tripleOccurences.length) {
          tripleOccurences = extendArray(tripleOccurences, iriParts.length);
        }
        for (int i = 0; i < iriParts.length; i++) {
          String prefix = getIriPrefix(iriParts, i);
          int targetChunk = computeHash(prefix) % numberOfGraphChunks;
          if (targetChunk < 0) {
            targetChunk *= -1;
          }
          if (i == (iriParts.length - 1)) {
            // this is the last hierarchy level
            for (int futureLevel = i + 1; futureLevel < tripleOccurences.length; futureLevel++) {
              tripleOccurences[futureLevel][targetChunk][0]++;
              tripleOccurences[futureLevel][targetChunk][1]++;
            }
          } else {
            tripleOccurences[i + 1][targetChunk][0]++;
          }
        }
      }
    }
    return tripleOccurences;
  }

  private long[][][] extendArray(long[][][] tripleOccurences, int newLength) {
    long[][][] extendedArray = new long[newLength + 1][][];
    System.arraycopy(tripleOccurences, 0, extendedArray, 0, tripleOccurences.length);
    for (int i = tripleOccurences.length; i < extendedArray.length; i++) {
      extendedArray[i] = new long[extendedArray[0].length][2];
      if (i > 1) {
        for (int chunk = 0; chunk < extendedArray[i].length; chunk++) {
          extendedArray[i][chunk][0] = extendedArray[i - 1][chunk][1];
          extendedArray[i][chunk][1] = extendedArray[i - 1][chunk][1];
        }
      }
    }
    return extendedArray;
  }

  private String[] getIRIHierarchy(String iriStr) {
    if (iriStr.startsWith("<")) {
      iriStr = iriStr.substring(1);
      if (iriStr.endsWith(">")) {
        iriStr = iriStr.substring(0, iriStr.length() - 1);
      }
    }
    IRI iri = IRIResolver.parseIRI(iriStr);
    String host = iri.getRawHost();
    String[] hostParts = null;
    if (host != null) {
      hostParts = host.split(Pattern.quote("."));
    }
    String path = iri.getRawPath();
    String[] pathParts = null;
    if (path != null) {
      if (path.startsWith("/")) {
        path = path.substring(1);
      }
      if (path.endsWith("/")) {
        path = path.substring(0, path.length() - 1);
      }
      if (!path.isEmpty()) {
        pathParts = path.split(Pattern.quote("/"));
      }
    }
    String fragment = iri.getRawFragment();
    String query = iri.getRawQuery();

    int iriHierarchyLength = 0;
    iriHierarchyLength += hostParts == null ? 0 : hostParts.length;
    iriHierarchyLength += pathParts == null ? 0 : pathParts.length;
    iriHierarchyLength += (fragment == null) || fragment.isEmpty() ? 0 : 1;
    iriHierarchyLength += (query == null) || query.isEmpty() ? 0 : 1;

    String[] result = new String[iriHierarchyLength];
    int nextInsertionIndex = 0;
    if (hostParts != null) {
      for (int i = hostParts.length - 1; i >= 0; i--) {
        result[nextInsertionIndex++] = hostParts[i];
      }
    }
    if (pathParts != null) {
      for (String pathPart : pathParts) {
        result[nextInsertionIndex++] = pathPart;
      }
    }
    if ((fragment != null) && !fragment.isEmpty()) {
      result[nextInsertionIndex++] = fragment;
    }
    if ((query != null) && !query.isEmpty()) {
      result[nextInsertionIndex++] = query;
    }
    return result;
  }

  private String getIriPrefix(String[] iriParts, int hierarchyLevel) {
    StringBuilder sb = new StringBuilder("/");
    for (int i = 0; (i <= hierarchyLevel) && (i < iriParts.length); i++) {
      sb.append("/").append(iriParts[i]);
    }
    return sb.toString();
  }

}
