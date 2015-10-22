package de.uni_koblenz.west.cidre.master.dictionary.impl;

import java.io.File;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import de.uni_koblenz.west.cidre.master.dictionary.Dictionary;

public class MapDBDictionary implements Dictionary {

	private final Logger logger;

	private final DB database;

	private final ConcurrentMap<String, Long> encoder;

	public MapDBDictionary(MapDBStorageOptions storageType,
			MapDBDataStructureOptions dataStructure, String storageDir,
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
			switch (dataStructure) {
			case B_TREE_MAP:
				encoder = database.createHashMap("encoder")
						.keySerializer(new Serializer.CompressionWrapper<>(
								Serializer.STRING))
						.valueSerializer(Serializer.LONG).makeOrGet();
				break;
			case HASH_TREE_MAP:
			default:
				encoder = database.createHashMap("encoder")
						.keySerializer(new Serializer.CompressionWrapper<>(
								Serializer.STRING))
						.valueSerializer(Serializer.LONG).makeOrGet();
			}
		} finally {
			close();
		}
	}

	// TODO add decoder and couple them

	@Override
	public void close() {
		if (encoder instanceof HTreeMap) {
			((HTreeMap<String, Long>) encoder).close();
		} else {
			assert encoder instanceof BTreeMap;
			((BTreeMap<String, Long>) encoder).close();
		}
		database.close();
	}

}
