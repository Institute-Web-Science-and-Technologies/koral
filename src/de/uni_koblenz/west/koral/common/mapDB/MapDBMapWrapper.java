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
