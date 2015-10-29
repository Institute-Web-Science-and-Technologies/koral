package de.uni_koblenz.west.cidre.master.dictionary.impl;

import java.io.File;

import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeKeySerializer.BasicKeySerializer;
import org.mapdb.Serializer;

import de.uni_koblenz.west.cidre.common.mapDB.BTreeMapWrapper;
import de.uni_koblenz.west.cidre.common.mapDB.HashTreeMapWrapper;
import de.uni_koblenz.west.cidre.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.cidre.common.mapDB.MapDBDataStructureOptions;
import de.uni_koblenz.west.cidre.common.mapDB.MapDBMapWrapper;
import de.uni_koblenz.west.cidre.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.cidre.master.dictionary.Dictionary;

public class MapDBDictionary implements Dictionary {

	private final MapDBMapWrapper<String, Long> encoder;

	private final MapDBMapWrapper<Long, String> decoder;

	private long nextID = 0;

	private final long maxID = 0x0000ffffffffffffl;

	@SuppressWarnings("unchecked")
	public MapDBDictionary(MapDBStorageOptions storageType,
			MapDBDataStructureOptions dataStructure, String storageDir,
			boolean useTransactions, boolean writeAsynchronously,
			MapDBCacheOptions cacheType) {
		File dictionaryDir = new File(storageDir);
		if (!dictionaryDir.exists()) {
			dictionaryDir.mkdirs();
		}
		try {
			switch (dataStructure) {
			case B_TREE_MAP:
				encoder = new BTreeMapWrapper<>(storageType,
						dictionaryDir.getAbsolutePath() + File.separatorChar
								+ "encoder.db",
						useTransactions, writeAsynchronously, cacheType,
						"encoder", BTreeKeySerializer.STRING, Serializer.LONG,
						false);
				decoder = new BTreeMapWrapper<>(storageType,
						dictionaryDir.getAbsolutePath() + File.separatorChar
								+ "decoder.db",
						useTransactions, writeAsynchronously, cacheType,
						"decoder", BasicKeySerializer.BASIC, Serializer.STRING,
						true);
				break;
			case HASH_TREE_MAP:
			default:
				encoder = new HashTreeMapWrapper<>(storageType,
						dictionaryDir.getAbsolutePath() + File.separatorChar
								+ "encoder.db",
						useTransactions, writeAsynchronously, cacheType,
						"encoder",
						new Serializer.CompressionWrapper<>(Serializer.STRING),
						Serializer.LONG);
				decoder = new HashTreeMapWrapper<>(storageType,
						dictionaryDir.getAbsolutePath() + File.separatorChar
								+ "decoder.db",
						useTransactions, writeAsynchronously, cacheType,
						"decoder", Serializer.LONG,
						new Serializer.CompressionWrapper<>(Serializer.STRING));
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
				if (usedIds != null) {
					long id = usedIds.longValue();
					// delete ownership
					id = id << 16;
					id = id >>> 16;
					if (id >= nextID) {
						nextID = id + 1;
					}
				}
			}
		} catch (Throwable e) {
			close();
			throw e;
		}
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
	public boolean isEmpty() {
		return nextID == 0;
	}

	@Override
	public void clear() {
		if (encoder != null) {
			encoder.clear();
		}
		if (decoder != null) {
			decoder.clear();
		}
		nextID = 0;
	}

	@Override
	public void close() {
		if (encoder != null) {
			encoder.close();
		}
		if (decoder != null) {
			decoder.close();
		}
	}

}
