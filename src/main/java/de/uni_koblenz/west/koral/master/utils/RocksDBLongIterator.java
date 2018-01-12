package de.uni_koblenz.west.koral.master.utils;

import org.rocksdb.RocksIterator;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;

import java.util.NoSuchElementException;

/**
 * Iterator over a RocksDB instance whose key and value is one long value each.
 * It returns key value pairs.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class RocksDBLongIterator implements AutoCloseable {

  private RocksIterator iterator;

  private byte[][] next;

  public RocksDBLongIterator(RocksIterator iterator) {
    this.iterator = iterator;
    iterator.seekToFirst();
    next = getNext();
  }

  public boolean hasNext() {
    if (next == null) {
      iterator.close();
      iterator = null;
    }
    return next != null;
  }

  public long[] next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    byte[][] nextReturned = next;
    next = getNext();
    return new long[] { NumberConversion.bytes2long(nextReturned[0]),
            NumberConversion.bytes2long(nextReturned[1]) };
  }

  private byte[][] getNext() {
    if (!iterator.isValid()) {
      return null;
    } else {
      byte[][] result = new byte[][] { iterator.key(), iterator.value() };
      iterator.next();
      return result;
    }
  }

  @Override
  public void close() {
    if (iterator != null) {
      iterator.close();
    }
  }

}
