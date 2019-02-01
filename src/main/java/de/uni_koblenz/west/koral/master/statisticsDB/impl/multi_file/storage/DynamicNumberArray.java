package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.Utils;

/**
 * Wraps a byte array and allows index access for (numerical) values that may consist of more than one array field. This
 * means that values can be initially stored as bytes with the corresponsing limit of +127, and as they grow, their size
 * can grow dynamically on demand, so that at the first upgrade each value is stored in two bytes. At all times, every
 * value has the same size as all other values.
 *
 * @author Philipp Töws
 *
 */
public class DynamicNumberArray {

	private final int capacity;

	private byte[] array;

	/**
	 * The current size of a value in bytes.
	 */
	private int valueSize;

	public DynamicNumberArray(int capacity) {
		this.capacity = capacity;

		array = new byte[capacity];
		valueSize = 1;
	}

	public DynamicNumberArray(byte[] array, int valueSize) {
		this.array = array;
		this.valueSize = valueSize;
		capacity = array.length / valueSize;
	}

	public long get(int index) {
		int offset = index * valueSize;
		return NumberConversion.signedBytes2long(array, offset, valueSize);
	}

	public void set(int index, long value) {
		int offset = index * valueSize;
		long currentMaxValue = (1 << ((Byte.SIZE * valueSize) - 1)) - 1;
		long currentMinValue = -currentMaxValue - 1;
		if ((value >= currentMaxValue) || (value <= currentMinValue)) {
			upgrade(Utils.neededBytesForValue(value, true));
		}
		NumberConversion.signedLong2bytes(value, array, offset, valueSize);
	}

	private void upgrade(int newValueSize) {
		byte[] newArray = new byte[capacity * newValueSize];
		for (int i = 0; i < capacity; i++) {
			NumberConversion.signedLong2bytes(get(i), newArray, i * newValueSize, newValueSize);
		}
		array = newArray;
		valueSize = newValueSize;
	}

}
