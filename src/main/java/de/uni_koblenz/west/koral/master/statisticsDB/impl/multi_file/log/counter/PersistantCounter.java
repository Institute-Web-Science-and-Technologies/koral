package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.counter;

import java.util.Iterator;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;

/**
 * Counter that uses a {@link PersistantStore} as storage backend.
 *
 * @author Philipp Töws
 *
 */
public class PersistantCounter implements Counter {

	private final PersistantStore store;

	public PersistantCounter(PersistantStore store) {
		this.store = store;
	}

	@Override
	public void countFor(long element) {
		byte[] elementBytes = NumberConversion.long2bytes(element);
		byte[] frequencyArray = store.get(elementBytes);
		if (frequencyArray == null) {
			frequencyArray = NumberConversion.long2bytes(1L);
		} else {
			frequencyArray = NumberConversion
					.long2bytes(NumberConversion.bytes2long(frequencyArray) + 1);
		}
		store.put(elementBytes, frequencyArray);
	}

	@Override
	public long getFrequency(long element) {
		byte[] elementBytes = NumberConversion.long2bytes(element);
		byte[] frequencyArray = store.get(elementBytes);
		if (frequencyArray == null) {
			return 0;
		} else {
			return NumberConversion.bytes2long(frequencyArray);
		}
	}

	@Override
	public Iterator<Long> iterator() {
		Iterator<byte[]> iterator = store.iterator();
		return new Iterator<Long>() {

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public Long next() {
				return NumberConversion.bytes2long(iterator.next());
			}

		};
	}

	public void flush() {
		store.flush();
	}

	@Override
	public void reset() {
		store.reset();
	}

	public void delete() {
		store.delete();
	}

	@Override
	public void close() {
		store.close();
	}

}
