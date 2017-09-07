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

import org.apache.jena.graph.Node;

import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedFileOutputStream;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.measurement.MeasurementType;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.koral.master.graph_cover_creator.GraphCoverCreator;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Logger;

/**
 * provides the base implementation for all {@link GraphCoverCreator}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public abstract class GraphCoverCreatorBase implements GraphCoverCreator {

  protected final Logger logger;

  protected final MeasurementCollector measurementCollector;

  protected long[] numberOfTriplesPerChunk;

  public GraphCoverCreatorBase(Logger logger, MeasurementCollector measurementCollector) {
    this.logger = logger;
    this.measurementCollector = measurementCollector;
  }

  @Override
  public File[] createGraphCover(DictionaryEncoder dictionary, File rdfFile, File workingDir,
          int numberOfGraphChunks) {
    File[] chunkFiles = getGraphChunkFiles(workingDir, numberOfGraphChunks);
    EncodedFileOutputStream[] outputs = getOutputStreams(chunkFiles);
    boolean[] writtenFiles = new boolean[chunkFiles.length];
    try {
      try (EncodedFileInputStream input = new EncodedFileInputStream(getRequiredInputEncoding(),
              rdfFile);) {
        createCover(dictionary, input, numberOfGraphChunks, outputs, writtenFiles, workingDir);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } finally {
      for (EncodedFileOutputStream stream : outputs) {
        try {
          if (stream != null) {
            stream.close();
          }
        } catch (IOException e) {
        }
      }
      // delete empty chunks
      for (int i = 0; i < chunkFiles.length; i++) {
        if (!writtenFiles[i]) {
          if ((chunkFiles[i] != null) && chunkFiles[i].exists()) {
            chunkFiles[i].delete();
            chunkFiles[i] = null;
          }
        }
      }
    }
    if (measurementCollector != null) {
      long totalNumberOfTriples = 0;
      String[] numberOfChunkTriples = new String[numberOfTriplesPerChunk.length];
      for (int i = 0; i < numberOfTriplesPerChunk.length; i++) {
        totalNumberOfTriples += numberOfTriplesPerChunk[i];
        numberOfChunkTriples[i] = Long.toString(numberOfTriplesPerChunk[i]);
      }
      measurementCollector.measureValue(MeasurementType.TOTAL_GRAPH_SIZE, totalNumberOfTriples);
      measurementCollector.measureValue(MeasurementType.INITIAL_CHUNK_SIZES, numberOfChunkTriples);
    }
    return chunkFiles;
  }

  /**
   * The input graph is <code>rdfFiles</code> the triples or quadruples have to
   * be assigned to at least one graph chunk. After assigning it, it has to be
   * written into the according file
   * {@link GraphCoverCreatorBase#writeStatementToChunk(int, int, Node[], OutputStream[], boolean[])}
   * . Blank nodes have to be encoded via {@link #transformBlankNodes(Node[])} .
   * 
   * @param dictionary
   * @param input
   *          input {@link File} containing triples or quadruples
   * @param numberOfGraphChunks
   * @param outputs
   *          the {@link EncodedFileOutputStream}s that are used to write the
   *          output files
   * @param writtenFiles
   *          has to be set to true, if a triple is written to a specific file
   *          (chunk)
   * @param workingDir
   */
  protected abstract void createCover(DictionaryEncoder dictionary, EncodedFileInputStream input,
          int numberOfGraphChunks, EncodedFileOutputStream[] outputs, boolean[] writtenFiles,
          File workingDir);

  protected void writeStatementToChunk(int targetChunk, int numberOfGraphChunks,
          Statement statement, EncodedFileOutputStream[] outputs, boolean[] writtenFiles) {
    if (measurementCollector != null) {
      if (numberOfTriplesPerChunk == null) {
        numberOfTriplesPerChunk = new long[numberOfGraphChunks];
      }
      numberOfTriplesPerChunk[targetChunk]++;
    }
    Statement outputStatement = Statement.getStatement(getRequiredInputEncoding(),
            statement.getSubject(), statement.getProperty(), statement.getObject(),
            setContainment(targetChunk, statement.getContainment()));
    try {
      outputs[targetChunk].writeStatement(outputStatement);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    writtenFiles[targetChunk] = true;
  }

  private byte[] setContainment(int targetChunk, byte[] containment) {
    int bitsetIndex = targetChunk / Byte.SIZE;
    byte bitsetMask = getBitMaskFor(targetChunk + 1);
    containment[bitsetIndex] |= bitsetMask;
    return containment;
  }

  private byte getBitMaskFor(int computerId) {
    computerId -= 1;
    switch (computerId % Byte.SIZE) {
      case 0:
        return (byte) 0x80;
      case 1:
        return (byte) 0x40;
      case 2:
        return (byte) 0x20;
      case 3:
        return (byte) 0x10;
      case 4:
        return (byte) 0x08;
      case 5:
        return (byte) 0x04;
      case 6:
        return (byte) 0x02;
      case 7:
        return (byte) 0x01;
    }
    return 0;
  }

  private EncodedFileOutputStream[] getOutputStreams(File[] chunkFiles) {
    EncodedFileOutputStream[] outputs = new EncodedFileOutputStream[chunkFiles.length];
    for (int i = 0; i < outputs.length; i++) {
      try {
        outputs[i] = new EncodedFileOutputStream(chunkFiles[i]);
      } catch (IOException e) {
        for (int j = i; i >= 0; j--) {
          if (outputs[j] != null) {
            try {
              outputs[j].close();
            } catch (IOException e1) {
            }
          }
        }
        throw new RuntimeException(e);
      }
    }
    return outputs;
  }

  @Override
  public File[] getGraphChunkFiles(File workingDir, int numberOfGraphChunks) {
    File[] chunkFiles = new File[numberOfGraphChunks];
    for (int i = 0; i < chunkFiles.length; i++) {
      chunkFiles[i] = new File(
              workingDir.getAbsolutePath() + File.separatorChar + "chunk" + i + ".nq.gz");
    }
    return chunkFiles;
  }

  @Override
  public void close() {
  }

}
