package de.uni_koblenz.west.cidre.common.mapDB;

import java.io.Closeable;
import java.util.concurrent.ConcurrentMap;

import org.mapdb.DB;
import org.mapdb.DBMaker;

public abstract class MapDBMapWrapper<K, V>
		implements Closeable, ConcurrentMap<K, V> {

	protected final DB database;

	public MapDBMapWrapper(MapDBStorageOptions storageType, String databaseFile,
			boolean useTransactions, boolean writeAsynchronously,
			MapDBCacheOptions cacheType) {
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
