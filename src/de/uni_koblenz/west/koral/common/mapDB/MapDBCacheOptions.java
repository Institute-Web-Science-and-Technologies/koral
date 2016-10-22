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

import org.mapdb.DBMaker;

/**
 * Options to configure the used instance cache of MapDB.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public enum MapDBCacheOptions {

  NONE {
    @Override
    public DBMaker<?> setCaching(DBMaker<?> dbmaker) {
      return dbmaker.cacheDisable();
    }
  },

  HASH_TABLE {
    @Override
    public DBMaker<?> setCaching(DBMaker<?> dbmaker) {
      return dbmaker;
    }
  },

  LEAST_RECENTLY_USED {
    @Override
    public DBMaker<?> setCaching(DBMaker<?> dbmaker) {
      return dbmaker.cacheLRUEnable();
    }
  },

  HARD_REFERENCE {
    @Override
    public DBMaker<?> setCaching(DBMaker<?> dbmaker) {
      return dbmaker.cacheHardRefEnable();
    }
  },

  SOFT_REFERENCE {
    @Override
    public DBMaker<?> setCaching(DBMaker<?> dbmaker) {
      return dbmaker.cacheSoftRefEnable();
    }
  },

  WEAK_REFERENCE {
    @Override
    public DBMaker<?> setCaching(DBMaker<?> dbmaker) {
      return dbmaker.cacheWeakRefEnable();
    }
  };

  public abstract DBMaker<?> setCaching(DBMaker<?> dbmaker);

}
