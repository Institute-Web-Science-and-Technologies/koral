package de.uni_koblenz.west.koral.master.utils;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;

import java.io.File;

/**
 * Counts the degree of a specific element.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class RocksDBDegreeCounter implements AutoCloseable {

  private RocksDB counter;

  public RocksDBDegreeCounter(String storageDir) {
    this(storageDir, 400);
  }

  public RocksDBDegreeCounter(String storageDir, int maxOpenFiles) {
    File dictionaryDir = new File(storageDir);
    if (!dictionaryDir.exists()) {
      dictionaryDir.mkdirs();
    }
    Options options = getOptions(maxOpenFiles);
    try {
      counter = RocksDB.open(options, storageDir + File.separator + "counter");
    } catch (RocksDBException e) {
      close();
      throw new RuntimeException(e);
    }
  }

  private Options getOptions(int maxOpenFiles) {
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setMaxOpenFiles(maxOpenFiles);
    options.setWriteBufferSize(64 * 1024 * 1024);
    return options;
  }

  public void countFor(long element) {
    byte[] valueArray = NumberConversion.long2bytes(element);
    countFor(valueArray);
  }

  public void countFor(byte[] element) {
    try {
      byte[] frequencyArray = counter.get(element);
      if (frequencyArray == null) {
        frequencyArray = NumberConversion.long2bytes(1L);
      } else {
        frequencyArray = NumberConversion
                .long2bytes(NumberConversion.bytes2long(frequencyArray) + 1);
      }
      counter.put(element, frequencyArray);
    } catch (RocksDBException e) {
      new RuntimeException(e);
    }
  }

  public long getFrequency(long element) {
    byte[] valueArray = NumberConversion.long2bytes(element);
    try {
      byte[] frequencyArray = counter.get(valueArray);
      if (frequencyArray == null) {
        return 0;
      } else {
        return NumberConversion.bytes2long(frequencyArray);
      }
    } catch (RocksDBException e) {
      new RuntimeException(e);
    }
    return -1;
  }

  public RocksDBLongIterator iterator() {
    return new RocksDBLongIterator(counter.newIterator());
  }

  @Override
  public void close() {
    if (counter != null) {
      counter.close();
    }
  }

}
