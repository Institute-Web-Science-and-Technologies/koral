package de.uni_koblenz.west.cidre.common.mapDB;

import org.mapdb.DBMaker;

import java.io.File;

/**
 * Configures the persistence of MapDB.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public enum MapDBStorageOptions {

  RANDOM_ACCESS_FILE {
    @Override
    public DBMaker<?> getDBMaker(String file) {
      return DBMaker.newFileDB(new File(file));
    }
  },

  MEMORY_MAPPED_FILE {
    @Override
    public DBMaker<?> getDBMaker(String file) {
      return DBMaker.newFileDB(new File(file)).mmapFileEnableIfSupported();
    }
  },

  MEMORY {
    @Override
    public DBMaker<?> getDBMaker(String file) {
      return DBMaker.newMemoryDB();
    }
  };

  public abstract DBMaker<?> getDBMaker(String file);

}
