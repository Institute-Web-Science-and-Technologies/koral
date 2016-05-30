package de.uni_koblenz.west.koral.master.dictionary.impl;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.dictionary.Dictionary;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class RocksDBDictionary implements Dictionary {

  public static final int DEFAULT_MAX_BATCH_SIZE = 100000;

  private final String storageDir;

  private RocksDB encoder;

  private WriteBatch encoderBatch;

  private RocksDB decoder;

  private WriteBatch decoderBatch;

  private Map<String, Long> entriesInBatch;

  private final int maxBatchEntries;

  /**
   * id 0 indicates that a string in a query has not been encoded yet
   */
  private long nextID = 1;

  private final long maxID = 0x0000ffffffffffffL;

  public RocksDBDictionary(String storageDir) {
    this(storageDir, RocksDBDictionary.DEFAULT_MAX_BATCH_SIZE);
  }

  public RocksDBDictionary(String storageDir, int maxBatchEntries) {
    this.maxBatchEntries = maxBatchEntries;
    this.storageDir = storageDir;
    File dictionaryDir = new File(storageDir);
    if (!dictionaryDir.exists()) {
      dictionaryDir.mkdirs();
    }
    Options options = getOptions();
    try {
      encoder = RocksDB.open(options, storageDir + File.separator + "encoder");
      decoder = RocksDB.open(options, storageDir + File.separator + "decoder");
    } catch (RocksDBException e) {
      close();
      throw new RuntimeException(e);
    }
  }

  private Options getOptions() {
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setMaxOpenFiles(800);
    options.setAllowOsBuffer(true);
    options.setWriteBufferSize(64 * 1024 * 1024);
    return options;
  }

  @Override
  public long encode(String value, boolean createNewEncodingForUnknownNodes) {
    byte[] valueBytes = null;
    try {
      valueBytes = value.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e1) {
      throw new RuntimeException(e1);
    }
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
    } catch (RocksDBException e) {
      close();
      throw new RuntimeException(e);
    }
    if (id == null) {
      if (nextID > maxID) {
        throw new RuntimeException("The maximum number of Strings have been encoded.");
      } else if (!createNewEncodingForUnknownNodes) {
        return 0;
      } else {
        id = NumberConversion.long2bytes(nextID);
        put(value, valueBytes, nextID, id);
        nextID++;
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
      encoderBatch = new WriteBatch();
    }
    encoderBatch.put(valueBytes, id);
    if (decoderBatch == null) {
      decoderBatch = new WriteBatch();
    }
    decoderBatch.put(id, valueBytes);
    if (entriesInBatch.size() == maxBatchEntries) {
      internalFlush();
    }
  }

  @Override
  public String decode(long id) {
    try {
      byte[] valueBytes = decoder.get(NumberConversion.long2bytes(id));
      if (valueBytes == null) {
        return null;
      }
      return new String(valueBytes, "UTF-8");
    } catch (RocksDBException | UnsupportedEncodingException e) {
      close();
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isEmpty() {
    return nextID == 1;
  }

  @Override
  public void flush() {
    internalFlush();
    try {
      encoder.compactRange();
      decoder.compactRange();
    } catch (RocksDBException e) {
      close();
      throw new RuntimeException(e);
    }
  }

  private void internalFlush() {
    try {
      WriteOptions writeOpts = new WriteOptions();
      if (encoderBatch != null) {
        encoder.write(writeOpts, encoderBatch);
        encoderBatch = null;
      }
      if (decoderBatch != null) {
        decoder.write(writeOpts, decoderBatch);
        decoderBatch = null;
      }
      if (entriesInBatch != null) {
        entriesInBatch.clear();
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clear() {
    close();
    Options options = getOptions();
    try {
      File encoderFile = new File(storageDir + File.separator + "encoder");
      deleteFile(encoderFile);
      encoder = RocksDB.open(options, encoderFile.getAbsolutePath());
      File decoderFile = new File(storageDir + File.separator + "decoder");
      deleteFile(decoderFile);
      decoder = RocksDB.open(options, decoderFile.getAbsolutePath());
    } catch (RocksDBException e) {
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
    internalFlush();
    if (encoder != null) {
      encoder.close();
    }
    if (decoder != null) {
      decoder.close();
    }
  }

}
