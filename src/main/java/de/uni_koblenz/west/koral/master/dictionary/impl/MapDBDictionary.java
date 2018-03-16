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
package de.uni_koblenz.west.koral.master.dictionary.impl;

import org.mapdb.BTreeKeySerializer;
import org.mapdb.Serializer;

import de.uni_koblenz.west.koral.common.mapDB.BTreeMapWrapper;
import de.uni_koblenz.west.koral.common.mapDB.HashTreeMapWrapper;
import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBDataStructureOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBMapWrapper;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.master.dictionary.Dictionary;

import java.io.File;

/**
 * Implements {@link Dictionary} with MapDB.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MapDBDictionary implements Dictionary {

  private final MapDBMapWrapper<String, Long> encoder;

  private final MapDBMapWrapper<Long, String> decoder;

  /**
   * id 0 indicates that a string in a query has not been encoded yet
   */
  private long nextID = 1;

  private final long maxID = 0x0000ffffffffffffl;

  @SuppressWarnings("unchecked")
  public MapDBDictionary(MapDBStorageOptions storageType, MapDBDataStructureOptions dataStructure,
          String storageDir, boolean useTransactions, boolean writeAsynchronously,
          MapDBCacheOptions cacheType) {
    File dictionaryDir = new File(storageDir);
    if (!dictionaryDir.exists()) {
      dictionaryDir.mkdirs();
    }
    try {
      switch (dataStructure) {
        case B_TREE_MAP:
          encoder = new BTreeMapWrapper<>(storageType,
                  dictionaryDir.getAbsolutePath() + File.separatorChar + "encoder.db",
                  useTransactions, writeAsynchronously, cacheType, "encoder",
                  BTreeKeySerializer.STRING, Serializer.LONG, false);
          decoder = new BTreeMapWrapper<>(storageType,
                  dictionaryDir.getAbsolutePath() + File.separatorChar + "decoder.db",
                  useTransactions, writeAsynchronously, cacheType, "decoder",
                  BTreeKeySerializer.BASIC, Serializer.STRING, true);
          break;
        case HASH_TREE_MAP:
        default:
          encoder = new HashTreeMapWrapper<>(storageType,
                  dictionaryDir.getAbsolutePath() + File.separatorChar + "encoder.db",
                  useTransactions, writeAsynchronously, cacheType, "encoder",
                  new Serializer.CompressionWrapper<>(Serializer.STRING), Serializer.LONG);
          decoder = new HashTreeMapWrapper<>(storageType,
                  dictionaryDir.getAbsolutePath() + File.separatorChar + "decoder.db",
                  useTransactions, writeAsynchronously, cacheType, "decoder", Serializer.LONG,
                  new Serializer.CompressionWrapper<>(Serializer.STRING));
      }
    } catch (Throwable e) {
      close();
      throw e;
    }
    resetNextId();
  }

  private void resetNextId() {
    try {
      for (Long usedIds : decoder.keySet()) {
        if (usedIds != null) {
          long id = usedIds.longValue();
          // delete ownership
          id = id << 16;
          id = id >>> 16;
          if (id >= nextID) {
            nextID = id + 1;
          }
        }
      }
    } catch (Throwable e) {
      close();
      throw e;
    }
  }

  @Override
  public long encode(String value, boolean createNewEncodingForUnknownNodes) {
    Long id = null;
    try {
      id = encoder.get(value);
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
          id = nextID;
          encoder.put(value, id);
          decoder.put(id, value);
          nextID++;
        } catch (Throwable e) {
          close();
          throw e;
        }
      }
    }
    return id.longValue();
  }

  @Override
  public String decode(long id) {
    try {
      return decoder.get(id);
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
  public long size() {
	  return nextID - 1;
  }
  
  @Override
  public void flush() {
  }

  @Override
  public void clear() {
    if (encoder != null) {
      encoder.clear();
    }
    if (decoder != null) {
      decoder.clear();
    }
    nextID = 1;
  }

  @Override
  public void close() {
    if (encoder != null) {
      encoder.close();
    }
    if (decoder != null) {
      decoder.close();
    }
  }

}
