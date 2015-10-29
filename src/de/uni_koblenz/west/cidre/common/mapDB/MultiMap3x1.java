package de.uni_koblenz.west.cidre.common.mapDB;

import java.util.Iterator;

import org.mapdb.BTreeKeySerializer;
import org.mapdb.Fun;

public class MultiMap3x1
		extends MultiMapMapDB<Fun.Tuple4<Long, Long, Long, byte[]>> {

	public MultiMap3x1(MapDBStorageOptions storageType, String databaseFile,
			boolean useTransactions, boolean writeAsynchronously,
			MapDBCacheOptions cacheType, String mapName) {
		super(storageType, databaseFile, useTransactions, writeAsynchronously,
				cacheType);
		multiMap = database.createTreeSet(mapName)
				.serializer(BTreeKeySerializer.TUPLE4).makeOrGet();
	}

	public boolean containsKey(long key1, long key2, long key3) {
		Iterator<byte[]> filter = Fun
				.filter(multiMap, (Long) key1, (Long) key2, (Long) key3)
				.iterator();
		return filter.hasNext();
	}

	public Iterable<byte[]> get(long key1, long key2, long key3) {
		return Fun.filter(multiMap, (Long) key1, (Long) key2, (Long) key3);
	}

	public void put(long key, long key2, long key3, byte[] containment) {
		multiMap.add(Fun.t4((Long) key, (Long) key2, (Long) key3, containment));
	}

	public void removeAll(long key1, long key2, long key3) {
		Iterator<byte[]> iterator = Fun
				.filter(multiMap, (Long) key1, (Long) key2, (Long) key3)
				.iterator();
		while (iterator.hasNext()) {
			iterator.next();
			iterator.remove();
		}
	}

	public void remove(long key1, long key2, long key3, byte[] containment) {
		multiMap.remove(
				Fun.t4((Long) key1, (Long) key2, (Long) key3, containment));
	}

}
