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

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedFileOutputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.dictionary.LongDictionary;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Creates a minimal edge-cut cover with the help of
 * <a href="http://glaros.dtc.umn.edu/gkhome/metis/metis/overview">METIS</a>
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MinimalEdgeCutCover extends MinimalEdgeCutOverCover {

  public MinimalEdgeCutCover(Logger logger, MeasurementCollector measurementCollector) {
    super(logger, measurementCollector);
  }

  @Override
  protected int getNumberOfPartitions(int lambda, long numberOfEdges, long numberOfVertices,
          int numberOfGraphChunks) {
    return numberOfGraphChunks;
  }

  @Override
  public EncodingFileFormat getRequiredInputEncoding() {
    return EncodingFileFormat.EEE;
  }

  @Override
  protected void createGraphCover(File encodedRDFGraph, LongDictionary dictionary,
          RocksDB vertex2chunkIndex, File ignoredTriples, EncodedFileOutputStream[] outputs,
          boolean[] writtenFiles, int numberOfGraphChunks, File workingDir) {
    // assign partitioned triples
    try (EncodedFileInputStream graphInput = new EncodedFileInputStream(getRequiredInputEncoding(),
            encodedRDFGraph);) {
      long lastVertex = -1;
      int lastChunkIndex = -1;

      for (Statement statement : graphInput) {
        long subject = dictionary.decodeLong(statement.getSubjectAsLong());
        long object = dictionary.decodeLong(statement.getObjectAsLong());
        Statement newStatement = Statement.getStatement(getRequiredInputEncoding(),
                NumberConversion.long2bytes(subject), statement.getProperty(),
                NumberConversion.long2bytes(object), statement.getContainment());

        int targetChunk = -1;
        if (subject == lastVertex) {
          targetChunk = lastChunkIndex;
        } else {
          byte[] targetBytes = vertex2chunkIndex.get(NumberConversion.long2bytes(subject));
          int target;
          if (targetBytes == null) {
            // if a vertex only occurs in a self loop or with a rdf:type
            // property it is not partitioned by
            // metis
            target = 0;
          } else {
            target = NumberConversion.bytes2int(targetBytes);
          }
          targetChunk = target;
          lastVertex = subject;
          lastChunkIndex = targetChunk;
        }

        writeStatementToChunk(targetChunk, numberOfGraphChunks, newStatement, outputs,
                writtenFiles);
      }
    } catch (IOException | RocksDBException e) {
      throw new RuntimeException(e);
    }

    if (ignoredTriples.exists()) {
      // assign ignored triples
      try (EncodedFileInputStream graphInput = new EncodedFileInputStream(
              getRequiredInputEncoding(), ignoredTriples);) {
        long lastVertex = -1;
        int lastChunkIndex = -1;

        for (Statement statement : graphInput) {
          long subject = statement.getSubjectAsLong();

          int targetChunk = -1;
          if (subject == lastVertex) {
            targetChunk = lastChunkIndex;
          } else {
            byte[] targetBytes = vertex2chunkIndex.get(NumberConversion.long2bytes(subject));
            int target;
            if (targetBytes == null) {
              // if a vertex only occurs in a self loop or with a rdf:type
              // property it is not partitioned by
              // metis
              target = 0;
            } else {
              target = NumberConversion.bytes2int(targetBytes);
            }
            targetChunk = target;
            lastVertex = subject;
            lastChunkIndex = targetChunk;
          }

          writeStatementToChunk(targetChunk, numberOfGraphChunks, statement, outputs, writtenFiles);
        }
      } catch (IOException | RocksDBException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
