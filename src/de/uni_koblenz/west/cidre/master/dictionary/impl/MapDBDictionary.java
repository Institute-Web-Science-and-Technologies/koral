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

	private long nextID = 0;

	private final long maxID = 0x0000ffffffffffffl;

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

		// create datastructure
		try {
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
		} catch (Throwable e) {
			close();
			throw e;
		}
		resetNextId();
	}

	private void resetNextId() {
		try {
			for (Long usedIds : decoder.keySet()) {
				if (usedIds != null && usedIds > nextID) {
					nextID = usedIds + 1;
				}
			}
		} catch (Throwable e) {
			close();
			throw e;
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
		Long id = null;
		try {
			id = encoder.get(value);
		} catch (Throwable e) {
			close();
			throw e;
		}
		if (id == null) {
			if (nextID > maxID) {
				throw new RuntimeException(
						"The maximum number of Strings have been encoded.");
			} else {
				try {
					id = nextID;
					encoder.put(value, id);
					decoder.put(id, value);
					nextID++;
				} catch (Throwable e) {
					close();
					throw e;
				}
			}
		}
		return id.longValue();
	}

	@Override
	public long setOwner(String value, short owner) {
		return setOwner(value, encode(value), owner);
	}

	@Override
	public long setOwner(long id, short owner) {
		return setOwner(decode(id), id, owner);
	}

	private long setOwner(String value, long oldID, short owner) {
		short oldOwner = (short) (oldID >>> 48);
		if (oldOwner != 0 && oldOwner != owner) {
			throw new IllegalArgumentException(
					"the first two bytes of the id must be 0 or equal to the new owner "
							+ owner);
		}
		if (oldOwner == owner) {
			return oldID;
		}
		long newID = owner;
		newID = newID << 48;
		newID |= oldID;

		try {
			encoder.replace(value, oldID, newID);
			decoder.remove(oldID, value);
			decoder.put(newID, value);
		} catch (Throwable e) {
			close();
			throw e;
		}

		return newID;
	}

	@Override
	public String decode(long id) {
		try {
			return decoder.get(id);
		} catch (Throwable e) {
			close();
			throw e;
		}
	}

	@Override
	public void clear() {
		if (encoder != null) {
			if (encoder instanceof HTreeMap) {
				((HTreeMap<String, Long>) encoder).clear();
			} else {
				assert encoder instanceof BTreeMap;
				((BTreeMap<String, Long>) encoder).clear();
			}
		}
		if (decoder != null) {
			if (decoder instanceof HTreeMap) {
				((HTreeMap<Long, String>) decoder).clear();
			} else {
				assert decoder instanceof BTreeMap;
				((BTreeMap<Long, String>) decoder).clear();
			}
		}
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
