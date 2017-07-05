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
package de.uni_koblenz.west.koral.slave.triple_store.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;

/**
 * A MapDB implementation of a multi map.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class RocksDBMultiMap implements MultiMap {

  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  private RocksDB multiMap;

  private final File rocksDBFile;

  private WriteBatch batch;

  private Set<ArrayWrapper> entriesInBatch;

  private final int maxBatchEntries;

  private final File numberOfTriplesFile;

  private long numberOfTriples;

  public RocksDBMultiMap(String databaseFile) {
    File indexDir = new File(databaseFile);
    if (!indexDir.exists()) {
      indexDir.mkdirs();
    }
    String storageDir = indexDir + File.separator + indexDir.getName();
    rocksDBFile = new File(storageDir);
    initializeDB(storageDir);
    maxBatchEntries = 100000;

    numberOfTriplesFile =
        new File(databaseFile + File.separator + indexDir.getName() + ".numberOfTriples");
    if (numberOfTriplesFile.exists()) {
      loadNumberOfTriples();
    }
  }

  private void initializeDB(String storageDir) {
    try {
      Options options = new Options();
      options.setCreateIfMissing(true);
      options.setMaxOpenFiles(100);
      options.setWriteBufferSize(64 * 1024 * 1024);
      multiMap = RocksDB.open(options, storageDir);
    } catch (RocksDBException e) {
      close();
      throw new RuntimeException(e);
    }
  }

  private void loadNumberOfTriples() {
    try (DataInputStream in = new DataInputStream(new FileInputStream(numberOfTriplesFile))) {
      numberOfTriples = in.readLong();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void saveNumberOfTriples() {
    try (DataOutputStream out = new DataOutputStream(new FileOutputStream(numberOfTriplesFile))) {
      out.writeLong(numberOfTriples);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int size() {
    return numberOfTriples > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) numberOfTriples;
  }

  @Override
  public boolean isEmpty() {
    return numberOfTriples <= 0;
  }

  @Override
  public boolean containsKey(byte[] prefix) {
    RocksIteratorKeyWrapper iterator = (RocksIteratorKeyWrapper) get(prefix).iterator();
    if (!iterator.hasNext()) {
      return false;
    }
    iterator.close();
    return true;
  }

  @Override
  public Iterable<byte[]> get(byte[] prefix) {
    RocksIterator iterator = multiMap.newIterator();
    return new RocksIteratorKeyWrapper(iterator, prefix);
  }

  @Override
  public void put(byte[] key) {
    if (entriesInBatch == null) {
      entriesInBatch = new HashSet<>();
    }
    boolean isNew = entriesInBatch.add(new ArrayWrapper(key));
    if (isNew) {
      if (batch == null) {
        batch = new WriteBatch();
      }
      batch.put(key, RocksDBMultiMap.EMPTY_BYTE_ARRAY);
      if (entriesInBatch.size() == maxBatchEntries) {
        internalFlush();
      }
    }
  }

  @Override
  public void removeAll(byte[] prefix) {
    for (byte[] key : get(prefix)) {
      remove(key);
    }
  }

  @Override
  public void remove(byte[] content) {
    if (batch == null) {
      batch = new WriteBatch();
    }
    batch.remove(content);
  }

  @Override
  public Iterator<byte[]> iterator() {
    RocksIterator iterator = multiMap.newIterator();
    return new RocksIteratorKeyWrapper(iterator, RocksDBMultiMap.EMPTY_BYTE_ARRAY).iterator();
  }

  @Override
  public void flush() {
    internalFlush();
    try {
      multiMap.compactRange();
    } catch (RocksDBException e) {
      close();
      throw new RuntimeException(e);
    }
  }

  private void internalFlush() {
    try {
      WriteOptions writeOpts = new WriteOptions();
      if (batch != null) {
        multiMap.write(writeOpts, batch);
        batch = null;
      }
      if (entriesInBatch != null) {
        entriesInBatch.clear();
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("size=").append(numberOfTriples).append("\n");
    String delim = "";
    for (byte[] triple : this) {
      sb.append(delim);
      sb.append("(");
      sb.append(NumberConversion.bytes2long(triple, 0 * Long.BYTES));
      sb.append(",");
      sb.append(NumberConversion.bytes2long(triple, 1 * Long.BYTES));
      sb.append(",");
      sb.append(NumberConversion.bytes2long(triple, 2 * Long.BYTES));
      sb.append(",");
      sb.append("{");
      int computerId = 0;
      String computerDelim = "";
      for (int i = 3 * Long.BYTES; i < triple.length; i++) {
        if ((triple[i] & 0x80) != 0) {
          sb.append(computerDelim);
          sb.append(computerId + 1);
          computerDelim = ",";
        }
        if ((triple[i] & 0x40) != 0) {
          sb.append(computerDelim);
          sb.append(computerId + 2);
          computerDelim = ",";
        }
        if ((triple[i] & 0x20) != 0) {
          sb.append(computerDelim);
          sb.append(computerId + 3);
          computerDelim = ",";
        }
        if ((triple[i] & 0x10) != 0) {
          sb.append(computerDelim);
          sb.append(computerId + 4);
          computerDelim = ",";
        }
        if ((triple[i] & 0x8) != 0) {
          sb.append(computerDelim);
          sb.append(computerId + 5);
          computerDelim = ",";
        }
        if ((triple[i] & 0x4) != 0) {
          sb.append(computerDelim);
          sb.append(computerId + 6);
          computerDelim = ",";
        }
        if ((triple[i] & 0x2) != 0) {
          sb.append(computerDelim);
          sb.append(computerId + 7);
          computerDelim = ",";
        }
        if ((triple[i] & 0x1) != 0) {
          sb.append(computerDelim);
          sb.append(computerId + 8);
          computerDelim = ",";
        }
        computerId += 8;
      }
      sb.append("}");
      sb.append(")");
      delim = "\n";
    }
    return sb.toString();
  }

  @Override
  public void clear() {
    numberOfTriples = 0;
    close();
    delete(rocksDBFile);
    initializeDB(rocksDBFile.getAbsolutePath());
  }

  private void delete(File fileOrDir) {
    if (fileOrDir.exists()) {
      if (fileOrDir.isDirectory()) {
        for (File file : fileOrDir.listFiles()) {
          delete(file);
        }
      }
      fileOrDir.delete();
    }
  }

  @Override
  public void close() {
    saveNumberOfTriples();
    internalFlush();
    if (multiMap != null) {
      multiMap.close();
    }
  }
}
