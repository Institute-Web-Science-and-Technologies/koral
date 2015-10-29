package de.uni_koblenz.west.cidre.common.mapDB;

import java.util.Iterator;

import org.mapdb.BTreeKeySerializer;
import org.mapdb.Fun;
import org.mapdb.Fun.Tuple2;

public class MultiMap2x2 extends
		MultiMapMapDB<Fun.Tuple3<Long, Long, Fun.Tuple2<Long, byte[]>>> {

	public MultiMap2x2(MapDBStorageOptions storageType, String databaseFile,
			boolean useTransactions, boolean writeAsynchronously,
			MapDBCacheOptions cacheType, String mapName) {
		super(storageType, databaseFile, useTransactions, writeAsynchronously,
				cacheType);
		multiMap = database.createTreeSet(mapName)
				.serializer(BTreeKeySerializer.TUPLE3).makeOrGet();
	}

	public boolean containsKey(long key1, long key2) {
		Iterator<Tuple2<Long, byte[]>> filter = Fun
				.filter(multiMap, (Long) key1, (Long) key2).iterator();
		return filter.hasNext();
	}

	public Iterable<Tuple2<Long, byte[]>> get(long key1, long key2) {
		return Fun.filter(multiMap, (Long) key1, (Long) key2);
	}

	public void put(long key1, long key2, long value1, byte[] containment) {
		multiMap.add(Fun.t3((Long) key1, (Long) key2,
				Fun.t2((Long) value1, containment)));
	}

	public void removeAll(long key1, long key2) {
		Iterator<Tuple2<Long, byte[]>> iterator = Fun
				.filter(multiMap, (Long) key1, (Long) key2).iterator();
		while (iterator.hasNext()) {
			iterator.next();
			iterator.remove();
		}
	}

	public void remove(long key1, long key2, long value1, byte[] containment) {
		multiMap.remove(Fun.t3((Long) key1, (Long) key2,
				Fun.t2((Long) value1, containment)));
	}

}
