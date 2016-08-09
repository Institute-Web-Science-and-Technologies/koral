package de.uni_koblenz.west.koral.slave.triple_store.impl;

import org.rocksdb.RocksIterator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Wraps a {@link RocksIterator}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class RocksIteratorKeyWrapper implements Iterable<byte[]>, Iterator<byte[]> {

  private RocksIterator iterator;

  private final byte[] prefix;

  private byte[] next;

  public RocksIteratorKeyWrapper(RocksIterator iterator) {
    this(iterator, new byte[0]);
  }

  public RocksIteratorKeyWrapper(RocksIterator iterator, byte[] prefix) {
    this.iterator = iterator;
    this.prefix = prefix;
    iterator.seek(prefix);
    next = getNext();
  }

  @Override
  public boolean hasNext() {
    if ((next == null) && (iterator != null)) {
      iterator.dispose();
      iterator = null;
    }
    return next != null;
  }

  @Override
  public byte[] next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    byte[] nextReturned = next;
    next = getNext();
    return nextReturned;
  }

  private byte[] getNext() {
    if (!iterator.isValid()) {
      return null;
    }
    byte[] key = iterator.key();
    if (hasPrefix(key)) {
      byte[] copyOfKey = Arrays.copyOf(key, key.length);
      iterator.next();
      return copyOfKey;
    } else {
      return null;
    }
  }

  private boolean hasPrefix(byte[] key) {
    if (key.length < prefix.length) {
      return false;
    }
    for (int i = 0; i < prefix.length; i++) {
      if (prefix[i] != key[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Iterator<byte[]> iterator() {
    return this;
  }

  public void close() {
    if (iterator != null) {
      iterator.dispose();
    }
  }

}
