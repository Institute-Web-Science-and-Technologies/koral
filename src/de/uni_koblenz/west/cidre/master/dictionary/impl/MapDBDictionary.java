package de.uni_koblenz.west.cidre.master.dictionary.impl;

import java.util.logging.Logger;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import de.uni_koblenz.west.cidre.master.dictionary.Dictionary;

public class MapDBDictionary implements Dictionary {

	private final Logger logger;

	private final DB database;

	public MapDBDictionary(MapDBStorageOptions storageType, String storageDir,
			boolean useTransactions, boolean writeAsynchronously,
			MapDBCacheOptions cacheType, Logger logger) {
		this.logger = logger;
		DBMaker<?> dbmaker = storageType.getDBMaker(storageDir);
		if (!useTransactions) {
			dbmaker = dbmaker.transactionDisable().closeOnJvmShutdown();
		}
		if (writeAsynchronously) {
			dbmaker = dbmaker.asyncWriteEnable();
		}
		dbmaker = cacheType.setCaching(dbmaker);
		database = dbmaker.make();

		// TODO Auto-generated constructor stub
	}

	@Override
	public void close() {
		database.close();
	}

}
