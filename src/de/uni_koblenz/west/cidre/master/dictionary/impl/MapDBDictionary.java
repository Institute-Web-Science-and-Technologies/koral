package de.uni_koblenz.west.cidre.master.dictionary.impl;

import java.io.File;
import java.util.concurrent.ConcurrentMap;

import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeKeySerializer.BasicKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import de.uni_koblenz.west.cidre.master.dictionary.Dictionary;

public class MapDBDictionary implements Dictionary {

	private final DB encoderDatabase;

	private final ConcurrentMap<String, Long> encoder;

	private final DB decoderDatabase;

	private final ConcurrentMap<Long, String> decoder;

	public MapDBDictionary(MapDBStorageOptions storageType,
			MapDBDataStructureOptions dataStructure, String storageDir,
			boolean useTransactions, boolean writeAsynchronously,
			MapDBCacheOptions cacheType) {
		File dictionaryDir = new File(storageDir);
		if (!dictionaryDir.exists()) {
			dictionaryDir.mkdirs();
		}

		// create or load database
		encoderDatabase = getDatabase(storageType,
				dictionaryDir.getAbsolutePath() + File.separatorChar
						+ "encoder.db",
				useTransactions, writeAsynchronously, cacheType);
		decoderDatabase = getDatabase(storageType,
				dictionaryDir.getAbsolutePath() + File.separatorChar
						+ "decoder.db",
				useTransactions, writeAsynchronously, cacheType);

		try {
			// create datastructure
			switch (dataStructure) {
			case B_TREE_MAP:
				encoder = encoderDatabase.createTreeMap("encoder")
						.keySerializer(BTreeKeySerializer.STRING)
						.valueSerializer(Serializer.LONG).makeOrGet();
				decoder = decoderDatabase.createTreeMap("decoder")
						.keySerializer(BasicKeySerializer.BASIC)
						.valuesOutsideNodesEnable()
						.valueSerializer(Serializer.STRING).makeOrGet();
				break;
			case HASH_TREE_MAP:
			default:
				encoder = encoderDatabase.createHashMap("encoder")
						.keySerializer(new Serializer.CompressionWrapper<>(
								Serializer.STRING))
						.valueSerializer(Serializer.LONG).makeOrGet();
				decoder = decoderDatabase.createHashMap("decoder")
						.keySerializer(Serializer.LONG)
						.valueSerializer(new Serializer.CompressionWrapper<>(
								Serializer.STRING))
						.makeOrGet();
			}
		} finally {
			close();
		}
	}

	private DB getDatabase(MapDBStorageOptions storageType, String databaseFile,
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
		return dbmaker.make();
	}

	@Override
	public long encode(String value) {
		Long id = encoder.get(value);
		if (id == null) {
			// TODO Auto-generated method stub
		}
		return id.longValue();
	}

	@Override
	public String decode(long id) {
		return decoder.get(id);
	}

	@Override
	public void close() {
		if (encoder != null) {
			if (encoder instanceof HTreeMap) {
				((HTreeMap<String, Long>) encoder).close();
			} else {
				assert encoder instanceof BTreeMap;
				((BTreeMap<String, Long>) encoder).close();
			}
		}
		if (decoder != null) {
			if (decoder instanceof HTreeMap) {
				((HTreeMap<Long, String>) decoder).close();
			} else {
				assert decoder instanceof BTreeMap;
				((BTreeMap<Long, String>) decoder).close();
			}
		}
		encoderDatabase.close();
		decoderDatabase.close();
	}

}
