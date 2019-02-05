package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.Utils;

/**
 * Wraps a byte array and allows index access for (numerical) values that may consist of more than one array field. This
 * means that values can be initially stored as bytes with the corresponsing limit of +127 (signed), and as they grow,
 * their size can grow dynamically on demand, so that after the first upgrade each value is stored in two bytes and
 * allows numbers up to +32767 (signed). At all times, every value has the same size as all other values. Additionally
 * the capacity can grow if more space is needed.
 *
 * Note that it is assumed, that the values are used consecutively, which means that, starting from the left, after the
 * first encountered zero all following values are zero. This assumption is used to minimize the segment lengths in
 * copying and moving operations. If this assumption does not hold for a use case, this class can still be used. If an
 * existing array is given to the constructor, it ignores above assumption and looks for the last value != 0. After
 * that, the internal state of the last used index may quickly become invalid, but this means only that the performance
 * during copy/move operations is not as optimal as possible.
 *
 * @author Philipp Töws
 *
 */
public class DynamicNumberArray {

	private static final int DEFAULT_CAPACITY = 10;

	// 100 is a good value for quickly growing arrays
	public static final int DEFAULT_EXTENSION_LENGTH = 100;

	private final int capacity;

	/**
	 * How many additional values will be storeable after an extension
	 */
	private final int extensionLength;

	private byte[] array;

	/**
	 * The current size of a value in bytes.
	 */
	private int valueSize;

	/**
	 * The index of the last value that is not equal to zero, starting from zero. A value of -1 indicates no used
	 * values. Is used to increase performance by allowing to only work on the filled part.
	 */
	private int lastUsedIndex;

	public DynamicNumberArray() {
		this(DEFAULT_CAPACITY, DEFAULT_EXTENSION_LENGTH);
	}

	public DynamicNumberArray(int capacity, int extensionLength) {
		this.capacity = capacity;
		this.extensionLength = extensionLength;

		array = new byte[capacity];
		valueSize = 1;
		lastUsedIndex = -1;
	}

	public DynamicNumberArray(byte[] array, int valueSize, int extensionLength) {
		this.array = array;
		this.valueSize = valueSize;
		this.extensionLength = extensionLength;
		capacity = array.length / valueSize;

		if (this.array == null) {
			this.array = new byte[DEFAULT_CAPACITY];
		}
		lastUsedIndex = findLastUsedIndex(0);
	}

	public int getCapacity() {
		return capacity;
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
			// Recalculate offset with new valueSize
			offset = index * valueSize;
		}
		NumberConversion.signedLong2bytes(value, array, offset, valueSize);

		// Update lastUsedIndex if necessary
		if ((index > lastUsedIndex) && (value != 0)) {
			lastUsedIndex = index;
		} else if ((index == lastUsedIndex) && (value == 0)) {
			lastUsedIndex = findLastUsedIndex(index);
		}
	}

	private void upgrade(int newValueSize) {
		byte[] newArray = createExtendedArray(newValueSize);
		for (int i = 0; i <= lastUsedIndex; i++) {
			NumberConversion.signedLong2bytes(get(i), newArray, i * newValueSize, newValueSize);
		}
		array = newArray;
		valueSize = newValueSize;
	}

	/**
	 * Copies part of the array analogous to System.arraycopy().
	 *
	 * @param srcIndex
	 *            The index of the first value that will be copied
	 * @param destIndex
	 *            The index that will contain the first copied value
	 * @param length
	 *            How many values are copied
	 */
	public void copy(int srcIndex, int destIndex, int length) {
		copy(srcIndex, destIndex, length, array);
	}

	private void copy(int srcIndex, int destIndex, int length, byte[] target) {
		System.arraycopy(target, srcIndex * valueSize, array, destIndex * valueSize, length * valueSize);
		int last = (destIndex + length) - 1;
		if (last > lastUsedIndex) {
			// It is assumed that the copied segment does not end with a zero
			lastUsedIndex = last;
		}
	}

	/**
	 * Moves part of the array analogous to System.arraycopy() but also clears everything of the source segment that is
	 * not overwritten.
	 *
	 * @param srcIndex
	 *            The index of the first value that will be moved
	 * @param destIndex
	 *            The index that will contain the first moved value
	 * @param length
	 *            How many values are moved
	 */
	public void move(int srcIndex, int destIndex, int length) {
		move(srcIndex, destIndex, length, array);
	}

	private void move(int srcIndex, int destIndex, int length, byte[] target) {
		copy(srcIndex, destIndex, length, target);

		// Clear fields that were not overwritten
		int startInclusive = 0, stopExclusive = 0;
		if (srcIndex < destIndex) {
			startInclusive = srcIndex * valueSize;
			stopExclusive = (destIndex * valueSize);
		} else if (srcIndex > destIndex) {
			startInclusive = (destIndex + length) * valueSize;
			stopExclusive = (srcIndex + length) * valueSize;
		}
		for (int i = startInclusive; i < stopExclusive; i++) {
			array[i] = 0;
		}

		// Update lastUsedIndex if necessary
		if ((srcIndex > destIndex) && (((srcIndex + length) - 1) >= lastUsedIndex)) {
			// The segment is moved to the left and includes the last used value
			// The new last used value is either the last overwritten value or the last value that was not moved
			lastUsedIndex = Math.max((destIndex + length) - 1, srcIndex - 1);
		}
	}

	/**
	 * Creates a gap in the array. All values to the right of <code>index </code> are moved to the right by
	 * <code>length</code>. The first empty value is at the given <code>index</code>, and the gap will consist of
	 * <code>length</code> empty values.
	 *
	 * @param index
	 *            The first value that will be empty
	 * @param length
	 *            How many empty values will be available in the created gap
	 */
	public void insertGap(int index, int length) {
		byte[] target = array;
		if ((index + length) > capacity) {
			target = createExtendedArray(valueSize);
			copy(0, 0, index, target);
		}
		move(index, (index + length) - 1, lastUsedIndex - index, target);
	}

	private byte[] createExtendedArray(int newValueSize) {
		return new byte[(capacity + extensionLength) * newValueSize];
	}

	/**
	 * Finds the index of the last value that is not equal to zero. Starting from the given index, the array is searched
	 * to the left direction as far as possible, and then to the right (again from the given position).
	 *
	 * @param startIndex
	 *            The first index that will be examined and used as starting position.
	 * @return Index of the last value != 0. Returns -1 if no values are used.
	 */
	protected int findLastUsedIndex(int startIndex) {
		int i = startIndex;
		// This outer loop will always run twice, with increment being either -1 or +1
		for (int increment = -1; increment <= 1; increment += 2) {
			for (; (i >= 0) && (i < array.length); i += increment) {
				if (array[i] != 0) {
					return i / valueSize;
				}
			}
			i = startIndex + 1;
		}
		return -1;
	}

}
