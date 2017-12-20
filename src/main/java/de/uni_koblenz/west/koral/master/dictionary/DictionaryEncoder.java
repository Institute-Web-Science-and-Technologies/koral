/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License along with Koral. If not,
 * see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
package de.uni_koblenz.west.koral.master.dictionary;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedFileOutputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.measurement.MeasurementType;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.common.utils.RDFFileIterator;
import de.uni_koblenz.west.koral.master.dictionary.impl.RocksDBDictionary;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.koral.master.utils.DeSerializer;

/**
 * <p>
 * This class encodes the created graph chunks. Thereby, it informs the {@link GraphStatistics}
 * component about the frequency of seen resources in the chunks and to determine the ownership of
 * each resource. Additionally, it provides the functionality to decode the resources later on
 * again.
 * </p>
 * 
 * <p>
 * {@link Dictionary} is used internally.
 * </p>
 * 
 * <p>
 * Resources are encoded using {@link DeSerializer}.
 * </p>
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class DictionaryEncoder implements Closeable {

  private final Logger logger;

  private final MeasurementCollector measurementCollector;

  private final Dictionary dictionary;

  public DictionaryEncoder(Configuration conf, Logger logger, MeasurementCollector collector) {
    this.logger = logger;
    measurementCollector = collector;
    if (conf != null) {
      dictionary =
          new RocksDBDictionary(conf.getDictionaryDir(true), conf.getMaxDictionaryWriteBatchSize());
    } else {
      dictionary = null;
    }
  }

  public File encodeOriginalGraphFiles(File[] plainGraphChunks, File workingDir,
      EncodingFileFormat outputFormat, int numberOfGraphChunks) {
    clear();
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_INITIAL_ENCODING_ENCODING_START,
          System.currentTimeMillis());
    }
    File result = getSemiEncodedGraphFile(workingDir);
    try (EncodedFileOutputStream out = new EncodedFileOutputStream(result);) {
      for (int i = 0; i < plainGraphChunks.length; i++) {
        if (plainGraphChunks[i] == null) {
          continue;
        }
        try (RDFFileIterator iter = new RDFFileIterator(plainGraphChunks[i], false, logger);) {
          for (Node[] quad : iter) {
            transformBlankNodes(quad);
            byte[] subject;
            if (outputFormat.isSubjectEncoded()) {
              subject = NumberConversion
                  .long2bytes(dictionary.encode(DeSerializer.serializeNode(quad[0]), true));
            } else {
              subject = DeSerializer.serializeNode(quad[0]).getBytes("UTF-8");
            }

            byte[] property;
            if (outputFormat.isPropertyEncoded()) {
              property = NumberConversion
                  .long2bytes(dictionary.encode(DeSerializer.serializeNode(quad[1]), true));
            } else {
              property = DeSerializer.serializeNode(quad[1]).getBytes("UTF-8");
            }

            byte[] object;
            if (outputFormat.isObjectEncoded()) {
              object = NumberConversion
                  .long2bytes(dictionary.encode(DeSerializer.serializeNode(quad[2]), true));
            } else {
              object = DeSerializer.serializeNode(quad[2]).getBytes("UTF-8");
            }

            int bitsetSize = numberOfGraphChunks / Byte.SIZE;
            if ((numberOfGraphChunks % Byte.SIZE) != 0) {
              bitsetSize += 1;
            }
            byte[] containment = new byte[bitsetSize];

            Statement statement =
                Statement.getStatement(outputFormat, subject, property, object, containment);
            out.writeStatement(statement);
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    dictionary.flush();
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_INITIAL_ENCODING_ENCODING_END,
          System.currentTimeMillis());
    }
    return result;
  }

  private void transformBlankNodes(Node[] statement) {
    for (int i = 0; i < statement.length; i++) {
      Node node = statement[i];
      if (node.isBlank()) {
        statement[i] =
            NodeFactory.createURI(Configuration.BLANK_NODE_URI_PREFIX + node.getBlankNodeId());
      }
    }
  }

  public File getSemiEncodedGraphFile(File workingDir) {
    File chunkFile =
        new File(workingDir.getAbsolutePath() + File.separatorChar + "inputRdfFile.senc.gz");
    return chunkFile;
  }

  public File[] encodeGraphChunksCompletely(File[] semiEncodedGraphChunks, File workingDir,
      EncodingFileFormat inputFormat) {
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_FINAL_ENCODING_ENCODING_START,
          System.currentTimeMillis());
    }
    File[] result = getFullyEncodedGraphChunks(workingDir, semiEncodedGraphChunks.length);
    for (int i = 0; i < semiEncodedGraphChunks.length; i++) {
      if (semiEncodedGraphChunks[i] == null) {
        result[i] = null;
        continue;
      }
      if (inputFormat == EncodingFileFormat.EEE) {
        // the input is already encoded completely
        if (!semiEncodedGraphChunks[i].equals(result[i])) {
          semiEncodedGraphChunks[i].renameTo(result[i]);
        }
        continue;
      }
      try (
          EncodedFileInputStream in =
              new EncodedFileInputStream(inputFormat, semiEncodedGraphChunks[i]);
          EncodedFileOutputStream out = new EncodedFileOutputStream(result[i]);) {
        for (Statement statement : in) {
          byte[] subject;
          if (statement.isSubjectEncoded()) {
            subject = statement.getSubject();
          } else {
            subject = NumberConversion
                .long2bytes(dictionary.encode(statement.getSubjectAsString(), true));
          }

          byte[] property;
          if (statement.isPropertyEncoded()) {
            property = statement.getProperty();
          } else {
            property = NumberConversion
                .long2bytes(dictionary.encode(statement.getPropertyAsString(), true));
          }

          byte[] object;
          if (statement.isObjectEncoded()) {
            object = statement.getObject();
          } else {
            object =
                NumberConversion.long2bytes(dictionary.encode(statement.getObjectAsString(), true));
          }

          byte[] containment = statement.getContainment();

          Statement outStatement = Statement.getStatement(EncodingFileFormat.EEE, subject, property,
              object, containment);
          out.writeStatement(outStatement);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    dictionary.flush();
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_FINAL_ENCODING_ENCODING_END,
          System.currentTimeMillis());
    }

    if (inputFormat != EncodingFileFormat.EEE) {
      // if the input was already encoded, then the input files were just
      // renamed
      for (File file : semiEncodedGraphChunks) {
        if (file != null) {
          file.delete();
        }
      }
    }
    return result;
  }

  public File[] getFullyEncodedGraphChunks(File workingDir, int numberOfGraphChunks) {
    File[] chunkFiles = new File[numberOfGraphChunks];
    for (int i = 0; i < chunkFiles.length; i++) {
      chunkFiles[i] =
          new File(workingDir.getAbsolutePath() + File.separatorChar + "chunk" + i + ".enc.gz");
    }
    return chunkFiles;
  }

  public Node decode(long id) {
    id = id & 0x00_00_ff_ff_ff_ff_ff_ffL;
    String plainText = dictionary.decode(id);
    if (plainText == null) {
      return null;
    }
    return DeSerializer.deserializeNode(plainText);
  }

  public long encodeWithoutOwnership(Node node, boolean createNewEncodingForUnknownNodes) {
    long id = dictionary.encode(DeSerializer.serializeNode(node), createNewEncodingForUnknownNodes);
    return id;
  }

  public long encode(Node node, boolean createNewEncodingForUnknownNodes,
      GraphStatistics statistics) {
    long id = dictionary.encode(DeSerializer.serializeNode(node), createNewEncodingForUnknownNodes);
    if (id == 0) {
      return id;
    }
    return statistics.getIDWithOwner(id);
  }

  public boolean isEmpty() {
    return dictionary.isEmpty();
  }
  
  public long size() {
	  return dictionary.size();
  }

  public void clear() {
    dictionary.clear();
  }

  @Override
  public void close() {
    dictionary.close();
  }

}
