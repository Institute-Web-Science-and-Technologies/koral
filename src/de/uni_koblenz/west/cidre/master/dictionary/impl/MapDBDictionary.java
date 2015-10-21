package de.uni_koblenz.west.cidre.master.dictionary.impl;

import java.io.File;
import java.util.logging.Logger;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import de.uni_koblenz.west.cidre.master.dictionary.Dictionary;

public class MapDBDictionary implements Dictionary {

	private final Logger logger;

	private final DB database;

	private final HTreeMap<String, Long> encoder;

	public MapDBDictionary(MapDBStorageOptions storageType, String storageDir,
			boolean useTransactions, boolean writeAsynchronously,
			MapDBCacheOptions cacheType, Logger logger) {
		this.logger = logger;
		// TODO create dictionary folder
		DBMaker<?> dbmaker = storageType
				.getDBMaker(storageDir + File.separatorChar + "encoder.db");
		if (!useTransactions) {
			dbmaker = dbmaker.transactionDisable().closeOnJvmShutdown();
		}
		if (writeAsynchronously) {
			dbmaker = dbmaker.asyncWriteEnable();
		}
		dbmaker = cacheType.setCaching(dbmaker);
		database = dbmaker.make();

		try {
			encoder = database.createHashMap("encoder")
					.keySerializer(new Serializer.CompressionWrapper<>(
							Serializer.STRING))
					.valueSerializer(Serializer.LONG).makeOrGet();
		} finally {
			close();
		}
	}

	// TODO add decoder and couple them

	@Override
	public void close() {
		encoder.close();
		database.close();
	}

}
