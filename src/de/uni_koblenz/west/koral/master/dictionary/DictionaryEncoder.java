package de.uni_koblenz.west.koral.master.dictionary;

import org.apache.jena.graph.Node;
import org.mapdb.Serializer;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.mapDB.HashTreeMapWrapper;
import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBMapWrapper;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.measurement.MeasurementType;
import de.uni_koblenz.west.koral.common.utils.RDFFileIterator;
import de.uni_koblenz.west.koral.master.dictionary.impl.MapDBDictionary;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.koral.master.utils.DeSerializer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
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

  public File[] encodeGraphChunks(File[] plainGraphChunks, GraphStatistics statistics,
          File workingDir) {
    File[] itermediateFiles = encodeGraphChunksAndCountStatistics(plainGraphChunks, statistics);
    return setOwnership(itermediateFiles, statistics, workingDir);
  }

  private File[] encodeGraphChunksAndCountStatistics(File[] plainGraphChunks,
          GraphStatistics statistics) {
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_ENCODING_ENCODING_START,
              System.currentTimeMillis());
    }
    File[] intermediateFiles = new File[plainGraphChunks.length];
    for (int i = 0; i < plainGraphChunks.length; i++) {
      if (plainGraphChunks[i] == null) {
        continue;
      }
      intermediateFiles[i] = new File(plainGraphChunks[i].getParentFile().getAbsolutePath()
              + File.separatorChar + "chunk" + i + ".enc.int.gz");
      try (RDFFileIterator iter = new RDFFileIterator(plainGraphChunks[i], true, logger);
              DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
                      new GZIPOutputStream(new FileOutputStream(intermediateFiles[i]))));) {
        for (Node[] quad : iter) {
          long subject = dictionary.encode(DeSerializer.serializeNode(quad[0]), true);
          long property = dictionary.encode(DeSerializer.serializeNode(quad[1]), true);
          long object = dictionary.encode(DeSerializer.serializeNode(quad[2]), true);
          statistics.count(subject, property, object, i);
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
    return intermediateFiles;
  }

  private File[] setOwnership(File[] intermediateFiles, GraphStatistics statistics,
          File workingDir) {
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_ENCODING_OWNERSHIP_START,
              System.currentTimeMillis());
    }
    File[] encodedFiles = new File[intermediateFiles.length];
    File tmpDir = new File(workingDir.getAbsolutePath() + File.separatorChar + "ownerMap");
    if (!tmpDir.exists()) {
      tmpDir.mkdirs();
    }
    try (MapDBMapWrapper<Long, Long> old2newId = new HashTreeMapWrapper<>(
            MapDBStorageOptions.MEMORY_MAPPED_FILE,
            tmpDir.getAbsolutePath() + File.separatorChar + "ownership", false, true,
            MapDBCacheOptions.LEAST_RECENTLY_USED, "ownershipMap", Serializer.LONG,
            Serializer.LONG);) {
      for (int i = 0; i < intermediateFiles.length; i++) {
        if (intermediateFiles[i] == null) {
          continue;
        }
        encodedFiles[i] = new File(intermediateFiles[i].getParentFile().getAbsolutePath()
                + File.separatorChar + "chunk" + i + ".enc.gz");
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(
                new GZIPInputStream(new FileInputStream(intermediateFiles[i]))));
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
                        new GZIPOutputStream(new FileOutputStream(encodedFiles[i]))));) {
          while (true) {
            long subject;
            try {
              subject = in.readLong();
            } catch (EOFException e) {
              // the end of the file has been reached
              break;
            }
            long property = in.readLong();
            long object = in.readLong();
            short containmentLength = in.readShort();
            byte[] containment = new byte[containmentLength];
            in.readFully(containment);

            long newSubject = adjustOwner(old2newId, statistics, subject);
            long newProperty = adjustOwner(old2newId, statistics, property);
            long newObject = adjustOwner(old2newId, statistics, object);

            out.writeLong(newSubject);
            out.writeLong(newProperty);
            out.writeLong(newObject);
            out.writeShort((short) containment.length);
            out.write(containment);
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        intermediateFiles[i].delete();
      }
    } finally {
      deleteDirectory(tmpDir);
    }
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_ENCODING_OWNERSHIP_END,
              System.currentTimeMillis());
      String[] ownerLoad = new String[encodedFiles.length];
      for (int i = 0; i < encodedFiles.length; i++) {
        ownerLoad[i] = Long.toString(statistics.getOwnerLoad(i));
      }
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_ENCODING_OWNERLOAD, ownerLoad);
    }
    return encodedFiles;
  }

  private void deleteDirectory(File tmpDir) {
    if (tmpDir.exists()) {
      for (File file : tmpDir.listFiles()) {
        file.delete();
      }
      tmpDir.delete();
    }
  }

  private long adjustOwner(MapDBMapWrapper<Long, Long> old2newId, GraphStatistics statistics,
          long id) {
    Long knownNewId = old2newId.get(id);
    if (knownNewId != null) {
      return knownNewId.longValue();
    }
    short newOwner = statistics.getOwner(id);
    long newId = dictionary.setOwner(id, newOwner);
    statistics.setOwner(id, newOwner);
    old2newId.put(id, newId);
    return newId;
  }

  public Node decode(long id) {
    String plainText = dictionary.decode(id);
    if (plainText == null) {
      return null;
    }
    return DeSerializer.deserializeNode(plainText);
  }

  public long encode(Node node, boolean createNewEncodingForUnknownNodes) {
    return dictionary.encode(DeSerializer.serializeNode(node), createNewEncodingForUnknownNodes);
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
