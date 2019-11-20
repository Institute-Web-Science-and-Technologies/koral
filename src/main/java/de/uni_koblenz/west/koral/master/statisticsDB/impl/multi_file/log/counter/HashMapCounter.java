package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.counter;

import java.util.HashMap;
import java.util.Iterator;

/**
 * Counter that uses a simple Java HashMap as storage.
 *
 * @author Philipp Töws
 *
 */
public class HashMapCounter implements Counter {

	private final HashMap<Long, Long> store;

	public HashMapCounter() {
		store = new HashMap<>();
	}

	@Override
	public Iterator<Long> iterator() {
		return store.keySet().iterator();
	}

	@Override
	public void countFor(long element) {
		Long frequency = store.get(element);
		if (frequency == null) {
			frequency = 1L;
		} else {
			frequency += 1;
		}
		store.put(element, frequency);
	}

	@Override
	public long getFrequency(long element) {
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
