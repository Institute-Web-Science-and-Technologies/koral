package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.dynamic_number_array;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.Utils;

/**
 * Wraps {@link DynamicNumberArray} by dynamically changing the backend implementation depending on the required size,
 * where each possible backend's per-element capacity are fixed and different.
 *
 * @author Philipp Töws
 *
 */
public class DynamicNumberArrayAccessor implements DynamicNumberArray {

	public static final int DEFAULT_CAPACITY = 10;

	// 1000 is a good value for quickly growing arrays
	public static final int DEFAULT_EXTENSION_LENGTH = 1000;

	private DynamicNumberArray dna;
	private final int initialValueSize;
	private final int initialCapacity;
	private final int extensionLength;

	private int valueSize;

	public DynamicNumberArrayAccessor() {
		this(DEFAULT_CAPACITY, 1, DEFAULT_EXTENSION_LENGTH);
	}

	public DynamicNumberArrayAccessor(int initialCapacity, int initialValueSize, int extensionLength) {
		this.initialCapacity = initialCapacity;
		this.initialValueSize = initialValueSize;
		this.extensionLength = extensionLength;

		reset();
	}

	public DynamicNumberArrayAccessor(long[] array, int initialValueSize, int extensionLength) {
		initialCapacity = array.length;
		this.initialValueSize = initialValueSize;
		this.extensionLength = extensionLength;
		if (initialValueSize == 1) {
			byte[] byteArray = new byte[array.length];
			for (int i = 0; i < array.length; i++) {
				byteArray[i] = (byte) array[i];
			}
			dna = new DynamicNumberByteArray(byteArray, extensionLength);
			valueSize = 1;
		} else if (initialValueSize == 2) {
			short[] shortArray = new short[array.length];
			for (int i = 0; i < array.length; i++) {
				shortArray[i] = (short) array[i];
			}
			dna = new DynamicNumberShortArray(shortArray, extensionLength);
			valueSize = 2;
		} else if ((initialValueSize > 2) && (initialValueSize <= 4)) {
			int[] intArray = new int[array.length];
			for (int i = 0; i < array.length; i++) {
				intArray[i] = (int) array[i];
			}
			dna = new DynamicNumberIntArray(intArray, extensionLength);
			valueSize = 4;
		} else if ((initialValueSize > 4) && (initialValueSize <= 8)) {
			long[] longArray = new long[array.length];
			for (int i = 0; i < array.length; i++) {
				longArray[i] = array[i];
			}
			dna = new DynamicNumberLongArray(longArray, extensionLength);
			valueSize = 8;
		} else {
			throw new IllegalArgumentException("Illegal value Size: " + initialValueSize);
		}

	}

	public void reset() {
		createDNA(initialCapacity, initialValueSize);
	}

	private void createDNA(int capacity, int newValueSize) {
		if (newValueSize == 1) {
			dna = new DynamicNumberByteArray(capacity, extensionLength);
			valueSize = 1;
		} else if (newValueSize == 2) {
			dna = new DynamicNumberShortArray(capacity, extensionLength);
			valueSize = 2;
		} else if ((newValueSize > 2) && (newValueSize <= 4)) {
			dna = new DynamicNumberIntArray(capacity, extensionLength);
			valueSize = 4;
		} else if ((newValueSize > 4) && (newValueSize <= 8)) {
			dna = new DynamicNumberLongArray(capacity, extensionLength);
			valueSize = 8;
		} else {
			throw new IllegalArgumentException("Illegal value Size: " + newValueSize);
		}
	}

	@Override
	public int capacity() {
		return dna.capacity();
	}

	@Override
	public long get(int index) {
		return dna.get(index);
	}

	@Override
	public void set(int index, long value, Caller caller) {
		long currentMaxValue = (1 << ((Byte.SIZE * valueSize) - 1)) - 1;
		long currentMinValue = -currentMaxValue - 1;
		if ((value > currentMaxValue) || (value < currentMinValue)) {
			upgrade(Utils.neededBytesForValue(value, true), caller);
		}
		dna.set(index, value, caller);
	}

	private void upgrade(int newValueSize, Caller caller) {
		DynamicNumberArray oldDna = dna;
		createDNA(oldDna.capacity(), newValueSize);
		for (int i = 0; i <= oldDna.getLastUsedIndex(); i++) {
			dna.set(i, oldDna.get(i), caller);
		}
	}

	@Override
	public void inc(int index, Caller caller) {
		set(index, get(index) + 1, caller);
	}

	@Override
	public void dec(int index, Caller caller) {
		set(index, get(index) - 1, caller);
	}

	@Override
	public void copy(int srcIndex, int destIndex, int length, Caller caller) {
		dna.copy(srcIndex, destIndex, length, caller);
	}

	@Override
	public void move(int srcIndex, int destIndex, Caller caller) {
		dna.move(srcIndex, destIndex, caller);
	}

	@Override
	public void insertGap(int index, int length, Caller caller) {
		dna.insertGap(index, length, caller);
	}

	@Override
	public int getLastUsedIndex() {
		return dna.getLastUsedIndex();
	}

	public byte[] getData() {
		// TODO
		return new byte[0];
	}

}
