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
package de.uni_koblenz.west.koral.common.mapDB;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.Closeable;
import java.util.concurrent.ConcurrentMap;

/**
 * This class wraps MapDB maps so that the Koral components become less
 * dependent on the used storage technology.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 * @param <K>
 * @param <V>
 */
public abstract class MapDBMapWrapper<K, V>
        implements Closeable, AutoCloseable, ConcurrentMap<K, V> {

  protected final DB database;

  public MapDBMapWrapper(MapDBStorageOptions storageType, String databaseFile,
          boolean useTransactions, boolean writeAsynchronously, MapDBCacheOptions cacheType) {
    DBMaker<?> dbmaker = storageType.getDBMaker(databaseFile);
    if (!useTransactions) {
      dbmaker = dbmaker.transactionDisable().closeOnJvmShutdown();
    }
    if (writeAsynchronously) {
      dbmaker = dbmaker.asyncWriteEnable();
    }
    dbmaker = cacheType.setCaching(dbmaker);
    database = dbmaker.make();
  }

  @Override
  public abstract void clear();

  @Override
  public void close() {
    if (!database.isClosed()) {
      database.close();
    }
  }

}
