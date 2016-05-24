package de.uni_koblenz.west.koral.master.dictionary.impl;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.impl.Iq80DBFactory;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.dictionary.Dictionary;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class LevelDBDictionary implements Dictionary {

  private final String storageDir;

  private DB encoder;

  private WriteBatch encoderBatch;

  private DB decoder;

  private WriteBatch decoderBatch;

  private Map<String, Long> entriesInBatch;

  private final int maxBatchEntries = 100000;

  /**
   * id 0 indicates that a string in a query has not been encoded yet
   */
  private long nextID = 1;

  private final long maxID = 0x0000ffffffffffffl;

  public LevelDBDictionary(String storageDir) {
    this.storageDir = storageDir;
    File dictionaryDir = new File(storageDir);
    if (!dictionaryDir.exists()) {
      dictionaryDir.mkdirs();
    }
    Options options = getOptions();
    try {
      encoder = Iq80DBFactory.factory.open(new File(storageDir + File.separator + "encoder"),
              options);
      decoder = Iq80DBFactory.factory.open(new File(storageDir + File.separator + "decoder"),
              options);
    } catch (IOException e) {
      close();
      throw new RuntimeException(e);
    }
  }

  private Options getOptions() {
    Options options = new Options();
    options.maxOpenFiles(128);
    options.cacheSize(100 * 1024 * 1024);
    options.writeBufferSize(100 * 1024 * 1024);
    options.createIfMissing(true);
    return options;
  }

  @Override
  public long encode(String value, boolean createNewEncodingForUnknownNodes) {
    byte[] valueBytes = Iq80DBFactory.bytes(value);
    byte[] id = null;
    try {
      // check cache, first
      if (entriesInBatch != null) {
        Long longId = entriesInBatch.get(value);
        if (longId != null) {
          id = NumberConversion.long2bytes(longId);
        }
      }
      if (id == null) {
        id = encoder.get(valueBytes);
      }
    } catch (Throwable e) {
      close();
      throw e;
    }
    if (id == null) {
      if (nextID > maxID) {
        throw new RuntimeException("The maximum number of Strings have been encoded.");
      } else if (!createNewEncodingForUnknownNodes) {
        return 0;
      } else {
        try {
          id = NumberConversion.long2bytes(nextID);
          put(value, valueBytes, nextID, id);
          nextID++;
        } catch (Throwable e) {
          close();
          throw e;
        }
      }
    }
    return NumberConversion.bytes2long(id);
  }

  private void put(String value, byte[] valueBytes, long longId, byte[] id) {
    if (entriesInBatch == null) {
      entriesInBatch = new HashMap<>();
    }
    entriesInBatch.put(value, longId);
    if (encoderBatch == null) {
      encoderBatch = encoder.createWriteBatch();
    }
    encoderBatch.put(valueBytes, id);
    if (decoderBatch == null) {
      decoderBatch = decoder.createWriteBatch();
    }
    decoderBatch.put(id, valueBytes);
    if (entriesInBatch.size() == maxBatchEntries) {
      flush();
    }
  }

  @Override
  public long setOwner(String value, short owner) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long setOwner(long id, short owner) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String decode(long id) {
    try {
      return Iq80DBFactory.asString(decoder.get(NumberConversion.long2bytes(id)));
    } catch (Throwable e) {
      close();
      throw e;
    }
  }

  @Override
  public boolean isEmpty() {
    return nextID == 1;
  }

  @Override
  public void flush() {
    if (encoderBatch != null) {
      encoder.write(encoderBatch);
      encoderBatch = null;
    }
    if (decoderBatch != null) {
      decoder.write(decoderBatch);
      decoderBatch = null;
    }
    if (entriesInBatch != null) {
      entriesInBatch.clear();
    }
  }

  @Override
  public void clear() {
    close();
    Options options = getOptions();
    try {
      File encoderFile = new File(storageDir + File.separator + "encoder");
      deleteFile(encoderFile);
      encoder = Iq80DBFactory.factory.open(encoderFile, options);
      File decoderFile = new File(storageDir + File.separator + "decoder");
      deleteFile(decoderFile);
      decoder = Iq80DBFactory.factory.open(decoderFile, options);
    } catch (IOException e) {
      close();
      throw new RuntimeException(e);
    }
    nextID = 1;
  }

  private void deleteFile(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        deleteFile(f);
      }
    }
    file.delete();
  }

  @Override
  public void close() {
    flush();
    try {
      if (encoder != null) {
        encoder.close();
      }
      if (decoder != null) {
        decoder.close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
