package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.counter.impl;

import java.util.HashMap;
import java.util.Iterator;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.counter.PersistantStore;

public class HashMapStore implements PersistantStore {

	private HashMap<byte[], byte[]> store;

	public HashMapStore() {
		store = new HashMap<>();
	}

	@Override
	public byte[] get(byte[] key) {
		return store.get(key);
	}

	@Override
	public void put(byte[] key, byte[] value) {
		store.put(key, value);
	}

	@Override
	public Iterator<byte[]> iterator() {
		return store.keySet().iterator();
	}

	@Override
	public void flush() {
		// Nothing to do
	}

	@Override
	public void reset() {
		store.clear();
	}

	@Override
	public void delete() {
		store.clear();
		store = null;
	}

	@Override
	public void close() {
		// Nothing to do
	}

}
