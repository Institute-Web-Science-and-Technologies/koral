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

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;

import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.NavigableSet;

/**
 * A MapDB implementation of a multi map.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MapDBMultiMap implements MultiMap {

  private final DB database;

  private final NavigableSet<byte[]> multiMap;

  private final File maxLengthFile;

  private int maxElementLength;

  public MapDBMultiMap(MapDBStorageOptions storageType, String databaseFile,
          boolean useTransactions, boolean writeAsynchronously, MapDBCacheOptions cacheType,
          String mapName) {
    assert (storageType != MapDBStorageOptions.MEMORY) || (databaseFile != null);
    DBMaker<?> dbmaker = storageType.getDBMaker(databaseFile);
    if (!useTransactions) {
      dbmaker = dbmaker.transactionDisable().closeOnJvmShutdown();
    }
    if (writeAsynchronously) {
      dbmaker = dbmaker.asyncWriteEnable();
    }
    dbmaker = cacheType.setCaching(dbmaker);
    database = dbmaker.make();

    multiMap = database.createTreeSet(mapName).comparator(Fun.BYTE_ARRAY_COMPARATOR).makeOrGet();

    maxLengthFile = new File(databaseFile + ".maxLength");
    if (maxLengthFile.exists()) {
      loadMaxLength();
    }
  }

  private void loadMaxLength() {
    try (DataInputStream in = new DataInputStream(new FileInputStream(maxLengthFile))) {
      maxElementLength = in.readInt();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void saveMaxLength() {
    try (DataOutputStream out = new DataOutputStream(new FileOutputStream(maxLengthFile))) {
      out.writeInt(maxElementLength);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int size() {
    return multiMap.size();
  }

  @Override
  public boolean isEmpty() {
    return multiMap.isEmpty();
  }

  @Override
  public boolean containsKey(byte[] prefix) {
    byte[] floor = multiMap.floor(prefix);
    return (floor != null) && isPrefix(prefix, floor);
  }

  private boolean isPrefix(byte[] prefix, byte[] element) {
    if (prefix.length > element.length) {
      return false;
    }
    for (int i = 0; i < prefix.length; i++) {
      if (prefix[i] != element[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Iterable<byte[]> get(byte[] prefix) {
    return multiMap.subSet(prefix, true, getMaxValue(prefix), true);
  }

  private byte[] getMaxValue(byte[] prefix) {
    byte[] max = new byte[maxElementLength];
    for (int i = 0; i < max.length; i++) {
      max[i] = i < prefix.length ? prefix[i] : Byte.MAX_VALUE;
    }
    return max;
  }

  @Override
  public void put(byte[] content) {
    if (content.length > maxElementLength) {
      maxElementLength = content.length;
    }
    multiMap.add(content);
  }

  @Override
  public void removeAll(byte[] prefix) {
    NavigableSet<byte[]> subSet = multiMap.subSet(prefix, true, getMaxValue(prefix), true);
    subSet.clear();
  }

  @Override
  public void remove(byte[] content) {
    multiMap.remove(content);
  }

  @Override
  public Iterator<byte[]> iterator() {
    return multiMap.iterator();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("maxElementLenght=").append(maxElementLength).append("\n");
    String delim = "";
    for (byte[] triple : multiMap) {
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
  public void flush() {
  }

  @Override
  public void clear() {
    multiMap.clear();
    maxElementLength = 0;
  }

  @Override
  public void close() {
    saveMaxLength();
    if (!database.isClosed()) {
      database.close();
    }
  }
}
