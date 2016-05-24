package de.uni_koblenz.west.koral.master.dictionary;

import org.apache.jena.graph.Node;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.measurement.MeasurementType;
import de.uni_koblenz.west.koral.common.utils.RDFFileIterator;
import de.uni_koblenz.west.koral.master.dictionary.impl.MapDBDictionary;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.koral.master.utils.DeSerializer;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

/**
 * <p>
 * This class encodes the created graph chunks. Thereby, it informs the
 * {@link GraphStatistics} component about the frequency of seen resources in
 * the chunks and to determine the ownership of each resource. Additionally, it
 * provides the functionality to decode the resources later on again.
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
      dictionary = new MapDBDictionary(conf.getDictionaryStorageType(),
              conf.getDictionaryDataStructure(), conf.getDictionaryDir(),
              conf.useTransactionsForDictionary(), conf.isDictionaryAsynchronouslyWritten(),
              conf.getDictionaryCacheType());
    } else {
      dictionary = null;
    }
  }

  public File[] encodeGraphChunks(File[] plainGraphChunks, File workingDir) {
    clear();
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_ENCODING_ENCODING_START,
              System.currentTimeMillis());
    }
    File[] result = new File[plainGraphChunks.length];
    for (int i = 0; i < plainGraphChunks.length; i++) {
      if (plainGraphChunks[i] == null) {
        continue;
      }
      result[i] = new File(plainGraphChunks[i].getParentFile().getAbsolutePath()
              + File.separatorChar + "chunk" + i + ".enc.int.gz");
      try (RDFFileIterator iter = new RDFFileIterator(plainGraphChunks[i], false, logger);
              DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
                      new GZIPOutputStream(new FileOutputStream(result[i]))));) {
        for (Node[] quad : iter) {
          long subject = dictionary.encode(DeSerializer.serializeNode(quad[0]), true);
          long property = dictionary.encode(DeSerializer.serializeNode(quad[1]), true);
          long object = dictionary.encode(DeSerializer.serializeNode(quad[2]), true);
          byte[] containment = DeSerializer.deserializeBitSetFromNode(quad[3]);

          out.writeLong(subject);
          out.writeLong(property);
          out.writeLong(object);
          out.writeShort((short) containment.length);
          out.write(containment);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_ENCODING_ENCODING_END,
              System.currentTimeMillis());
    }

    for (File file : plainGraphChunks) {
      file.delete();
    }
    return result;
  }

  public File[] getEncodedGraphChunks(File workingDir, int numberOfGraphChunks) {
    File[] chunkFiles = new File[numberOfGraphChunks];
    for (int i = 0; i < chunkFiles.length; i++) {
      chunkFiles[i] = new File(
              workingDir.getAbsolutePath() + File.separatorChar + "chunk" + i + ".enc.gz");
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

  public long encode(Node node, boolean createNewEncodingForUnknownNodes,
          GraphStatistics statistics) {
    long id = dictionary.encode(DeSerializer.serializeNode(node), createNewEncodingForUnknownNodes);
    if (id == 0) {
      return id;
    }
    short owner = statistics.getOwner(id);
    long newID = owner;
    newID = newID << 48;
    newID |= id;
    return newID;
  }

  public boolean isEmpty() {
    return dictionary.isEmpty();
  }

  public void clear() {
    dictionary.clear();
  }

  @Override
  public void close() {
    dictionary.close();
  }

}
