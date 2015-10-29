package de.uni_koblenz.west.cidre.common.mapDB;

import java.io.Closeable;
import java.util.Iterator;
import java.util.NavigableSet;

import org.mapdb.DB;
import org.mapdb.DBMaker;

public abstract class MultiMapMapDB<V>
		implements Closeable, AutoCloseable, Iterable<V> {

	protected final DB database;

	protected NavigableSet<V> multiMap;

	public MultiMapMapDB(MapDBStorageOptions storageType, String databaseFile,
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

	public int size() {
		return multiMap.size();
	}

	public boolean isEmpty() {
		return multiMap.isEmpty();
	}

	@Override
	public Iterator<V> iterator() {
		return multiMap.iterator();
	}

	public void clear() {
		multiMap.clear();
	}

	@Override
	public void close() {
		if (!database.isClosed()) {
			database.close();
		}
	}

}
