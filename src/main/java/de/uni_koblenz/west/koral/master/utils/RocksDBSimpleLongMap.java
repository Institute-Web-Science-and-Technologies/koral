package de.uni_koblenz.west.koral.master.utils;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

/**
 * A {@link SimpleLongMap} implemented with RocksDB
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class RocksDBSimpleLongMap extends CachedSimpleLongMap {

  private RocksDB map;

  public RocksDBSimpleLongMap(File contentFile, int numberOfOpenFiles) {
    super(contentFile);
    if (!contentFile.exists()) {
      contentFile.mkdirs();
    }
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setMaxOpenFiles(numberOfOpenFiles);
    options.setWriteBufferSize(64 * 1024 * 1024);
    try {
      map = RocksDB.open(options, contentFile.getAbsolutePath());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected byte[] internalGet(byte[] keyBytes) {
    try {
      return map.get(keyBytes);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void writeCache(Map<Long, byte[]> cache) {
    WriteBatch writeBatch = new WriteBatch();
    for (Entry<Long, byte[]> entry : cache.entrySet()) {
      writeBatch.put(NumberConversion.long2bytes(entry.getKey()), entry.getValue());
    }
    WriteOptions writeOpts = new WriteOptions();
    try {
      map.write(writeOpts, writeBatch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterator<long[]> iterator() {
    flush();
    return new Iterator<long[]>() {

      private final RocksIterator iterator;
      {
        iterator = map.newIterator();
        iterator.seekToFirst();
      }

      private long[] next = getNext();

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public long[] next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        long[] n = next;
        next = getNext();
        return n;
      }

      private long[] getNext() {
        if (iterator.isValid()) {
          long key = NumberConversion.bytes2long(iterator.key());
          long value = NumberConversion.bytes2long(iterator.value());
          iterator.next();
          return new long[] { key, value };
        } else {
          iterator.close();
          return null;
        }
      }
    };
  }

  @Override
  public void flush() {
    super.flush();
    try {
      map.compactRange();
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    map.close();
    super.close();
  }

}
