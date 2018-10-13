package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.counter;

import java.util.HashMap;
import java.util.Iterator;

public class HashMapCounter implements Counter<Long> {

	private final HashMap<Long, Long> store;

	public HashMapCounter() {
		store = new HashMap<>();
	}

	@Override
	public Iterator<Long> iterator() {
		return store.keySet().iterator();
	}

	@Override
	public void countFor(Long element) {
		Long frequency = store.get(element);
		if (frequency == null) {
			frequency = 1L;
		} else {
			frequency += 1;
		}
		store.put(element, frequency);
	}

	@Override
	public long getFrequency(Long element) {
		return store.get(element);
	}

	@Override
	public void reset() {
		store.clear();
	}

	@Override
	public void close() {
		store.clear();
	}

}
