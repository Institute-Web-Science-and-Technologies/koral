package de.uni_koblenz.west.cidre.common.mapDB;

import java.util.Iterator;
import java.util.NavigableSet;

import org.mapdb.BTreeKeySerializer;
import org.mapdb.Fun;

public class MultiMap1x3 extends
		MultiMapMapDB<Fun.Tuple2<Long, Fun.Tuple3<Long, Long, byte[]>>> {

	private final NavigableSet<Fun.Tuple2<Long, Fun.Tuple3<Long, Long, byte[]>>> multiMap;

	public MultiMap1x3(MapDBStorageOptions storageType, String databaseFile,
			boolean useTransactions, boolean writeAsynchronously,
			MapDBCacheOptions cacheType, String mapName) {
		super(storageType, databaseFile, useTransactions, writeAsynchronously,
				cacheType);
		multiMap = database.createTreeSet(mapName)
				.serializer(BTreeKeySerializer.TUPLE2).makeOrGet();
	}

	public boolean containsKey(long key) {
		Iterator<Fun.Tuple3<Long, Long, byte[]>> filter = Fun
				.filter(multiMap, (Long) key).iterator();
		return filter.hasNext();
	}

	public Iterable<Fun.Tuple3<Long, Long, byte[]>> get(long key) {
		return Fun.filter(multiMap, (Long) key);
	}

	public void put(long key, long value1, long value2, byte[] containment) {
		multiMap.add(Fun.t2((Long) key,
				Fun.t3((Long) value1, (Long) value2, containment)));
	}

	public void removeAll(long key) {
		Iterator<Fun.Tuple3<Long, Long, byte[]>> iterator = Fun
				.filter(multiMap, (Long) key).iterator();
		while (iterator.hasNext()) {
			iterator.next();
			iterator.remove();
		}
	}

	public void remove(long key, long value1, long value2, byte[] containment) {
		multiMap.remove(Fun.t2((Long) key,
				Fun.t3((Long) value1, (Long) value2, containment)));
	}

}
