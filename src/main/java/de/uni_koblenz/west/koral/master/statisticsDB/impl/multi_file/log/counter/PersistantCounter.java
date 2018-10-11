package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.counter;

import java.util.Iterator;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;

public class PersistantCounter implements Iterable<byte[]> {

	private final PersistantStore store;

	public PersistantCounter(PersistantStore store) {
		this.store = store;
	}

	public void countFor(byte[] element) {
		byte[] frequencyArray = store.get(element);
		if (frequencyArray == null) {
			frequencyArray = NumberConversion.long2bytes(1L);
		} else {
			frequencyArray = NumberConversion
					.long2bytes(NumberConversion.bytes2long(frequencyArray) + 1);
		}
		store.put(element, frequencyArray);
	}

	public long getFrequency(byte[] element) {
		byte[] frequencyArray = store.get(element);
		if (frequencyArray == null) {
			return 0;
		} else {
			return NumberConversion.bytes2long(frequencyArray);
		}
	}

	@Override
	public Iterator<byte[]> iterator() {
		return store.iterator();
	}

	public void flush() {
		store.flush();
	}

	public void close() {
		store.close();
	}

}
