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
  public synchronized boolean hasNext() {
    if ((next == null) && (iterator != null)) {
      iterator.close();
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

  private synchronized byte[] getNext() {
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

  public synchronized void close() {
    if (iterator != null) {
      iterator.close();
      iterator = null;
    }
  }

}
