package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.util.Arrays;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.SubbenchmarkManager;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.Utils;
import playground.StatisticsDBTest;

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
 * existing array is given to the constructor, it doesn't consider above assumption and looks for the last value != 0.
 * After that, the internal state of the last used index may quickly become invalid, but this means only that the
 * performance during copy/move operations is not as optimal as possible.
 *
 * @author Philipp Töws
 *
 */
public class DynamicNumberArray {

	public static final int DEFAULT_CAPACITY = 10;

	// 1000 is a good value for quickly growing arrays
	public static final int DEFAULT_EXTENSION_LENGTH = 1000;

	private int capacity;

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

	private final int initialValueSize;

	private final int initialCapacity;

	/**
	 * Only used to identify subbenchmarking tasks.
	 *
	 */
	public static enum Caller {
		NEXT,
		RELEASE
	}

	public DynamicNumberArray() {
		this(DEFAULT_CAPACITY, 1, DEFAULT_EXTENSION_LENGTH);
	}

	public DynamicNumberArray(int initialCapacity, int initialValueSize, int extensionLength) {
		this.initialCapacity = initialCapacity;
		this.initialValueSize = initialValueSize;
		this.extensionLength = extensionLength;

		reset();
	}

	/**
	 *
	 * @param array
	 *            Must not be null
	 * @param valueSize
	 * @param extensionLength
	 */
	public DynamicNumberArray(byte[] array, int initialValueSize, int extensionLength) {
		this.array = array;
		this.initialValueSize = initialValueSize;
		this.extensionLength = extensionLength;
		valueSize = initialValueSize;

		if (this.array == null) {
			throw new IllegalArgumentException("Array must not be null");
		}
		initialCapacity = this.array.length / valueSize;
		capacity = initialCapacity;
		lastUsedIndex = findLastUsedIndex(this.array);
	}

	public void reset() {
		capacity = initialCapacity;
		array = new byte[capacity];
		valueSize = initialValueSize;
		lastUsedIndex = -1;
	}

	public int capacity() {
		return capacity;
	}

	public long get(int index) {
		int offset = index * valueSize;
		return NumberConversion.signedBytes2long(array, offset, valueSize);
	}

	public void set(int index, long value, Caller caller) {
		int offset = index * valueSize;
		long currentMaxValue = (1 << ((Byte.SIZE * valueSize) - 1)) - 1;
		long currentMinValue = -currentMaxValue - 1;
		if ((value > currentMaxValue) || (value < currentMinValue)) {
			upgrade(capacity, Utils.neededBytesForValue(value, true), caller);
			// Recalculate offset with new valueSize
			offset = index * valueSize;
		}
		if (index > (capacity - 1)) {
			upgrade(index + 1, valueSize, caller);
		}
		NumberConversion.signedLong2bytes(value, array, offset, valueSize);

		// Update lastUsedIndex if necessary
		if ((index > lastUsedIndex) && (value != 0)) {
			lastUsedIndex = index;
		} else if ((index == lastUsedIndex) && (value == 0)) {
			lastUsedIndex = index - 1;
			// Use this if the array is not consecutive
			// lastUsedIndex = findLastUsedIndex(index);
		}
	}

	public void inc(int index, Caller caller) {
		set(index, get(index) + 1, caller);
	}

	public void dec(int index, Caller caller) {
		set(index, get(index) - 1, caller);
	}

	private void upgrade(int minCapacity, int newValueSize, Caller caller) {
		long start = 0;
		if (StatisticsDBTest.SUBBENCHMARKS) {
			start = System.nanoTime();
		}
		byte[] newArray = createExtendedArray(minCapacity, newValueSize);
		if (StatisticsDBTest.SUBBENCHMARKS) {
			subbenchmark(SubbenchmarkType.ALLOC, caller, System.nanoTime() - start);
		}
		if (newValueSize == valueSize) {
			start = 0;
			if (StatisticsDBTest.SUBBENCHMARKS) {
				start = System.nanoTime();
			}
			System.arraycopy(array, 0, newArray, 0, (lastUsedIndex + 1) * valueSize);
			if (StatisticsDBTest.SUBBENCHMARKS) {
				subbenchmark(SubbenchmarkType.ARRAYCOPY, caller, System.nanoTime() - start);
			}
		} else {
			start = 0;
			if (StatisticsDBTest.SUBBENCHMARKS) {
				start = System.nanoTime();
			}
			for (int i = 0; i <= lastUsedIndex; i++) {
				NumberConversion.signedLong2bytes(get(i), newArray, i * newValueSize, newValueSize);
			}
			if (StatisticsDBTest.SUBBENCHMARKS) {
				subbenchmark(SubbenchmarkType.ARRAYCOPY, caller, System.nanoTime() - start);
			}
		}
		array = newArray;
		valueSize = newValueSize;
		capacity = array.length / valueSize;
	}

	/**
	 * Copies part of the array analogous to System.arraycopy(). The capacity is increased if needed.
	 *
	 * @param srcIndex
	 *            The index of the first value that will be copied
	 * @param destIndex
	 *            The index that will contain the first copied value
	 * @param length
	 *            How many values are copied
	 */
	public void copy(int srcIndex, int destIndex, int length, Caller caller) {
		int last = (destIndex + length) - 1;
		if ((last + 1) > capacity) {
			upgrade(last + 1, valueSize, caller);
		}
		copy(srcIndex, destIndex, length, array, caller);
	}

	/**
	 * Copies part of the array into the <code>target</code> array analogous to System.arraycopy(). The capacity is NOT
	 * increased automatically. It is assumed that target will replace the internal array, and therefore the
	 * {@link #lastUsedIndex} might be set based on this operation.
	 *
	 * @param srcIndex
	 * @param destIndex
	 * @param length
	 * @param target
	 * @throws ArrayIndexOutOfBoundsException
	 *             If the target array has not enough capacity.
	 */
	private void copy(int srcIndex, int destIndex, int length, byte[] target, Caller caller) {
		int last = (destIndex + length) - 1;
		if (((last + 1) * valueSize) > target.length) {
			throw new ArrayIndexOutOfBoundsException("Target array needs at least a capacity of " + (last + 1));
		}
		long start = 0;
		if (StatisticsDBTest.SUBBENCHMARKS) {
			start = System.nanoTime();
		}
		System.arraycopy(array, srcIndex * valueSize, target, destIndex * valueSize, length * valueSize);
		if (StatisticsDBTest.SUBBENCHMARKS) {
			subbenchmark(SubbenchmarkType.ARRAYCOPY, caller, System.nanoTime() - start);
		}
		if (last > lastUsedIndex) {
			// It is assumed that the copied segment does not end with a zero
			lastUsedIndex = last;
		}
	}

	/**
	 * Moves part of the array analogous to System.arraycopy() but also clears everything of the source segment that is
	 * not overwritten. Moves all values right to (and including) srcIndex to destIndex. Uses internal lastUsedIndex
	 * value for optimization.
	 *
	 * For updating the lastUsedIndex it is assumed that the last element of the moved segment is not equal to zero.
	 *
	 * @param srcIndex
	 *            The index of the first value that will be moved
	 * @param destIndex
	 *            The index that will contain the first moved value
	 */
	public void move(int srcIndex, int destIndex, Caller caller) {
		move(srcIndex, destIndex, (lastUsedIndex - srcIndex) + 1, caller);
	}

	/**
	 * Moves part of the array analogous to System.arraycopy() but also clears everything of the source segment that is
	 * not overwritten. Moves all values right to (and including) srcIndex to destIndex. Uses internal lastUsedIndex
	 * value for optimization.
	 *
	 * For updating the lastUsedIndex it is assumed that the last element of the moved segment is not equal to zero.
	 *
	 * @param srcIndex
	 *            The index of the first value that will be moved
	 * @param destIndex
	 *            The index that will contain the first moved value
	 * @param length
	 *            How many values are moved
	 */
	private void move(int srcIndex, int destIndex, int length, Caller caller) {
		copy(srcIndex, destIndex, length, caller);

		// Clear fields that were not overwritten
		int startInclusive = 0, stopExclusive = 0;
		if (srcIndex < destIndex) {
			if ((destIndex - srcIndex) >= length) {
				// [S,S,X,D,D]
				startInclusive = srcIndex;
				stopExclusive = srcIndex + length;
			} else {
				// [S,S,SD,D,D]
				startInclusive = srcIndex;
				stopExclusive = destIndex;
			}
		} else if (srcIndex > destIndex) {
			if ((srcIndex - destIndex) >= length) {
				// [D,D,X,S,S]
				startInclusive = srcIndex;
				stopExclusive = srcIndex + length;
			} else {
				// [D,D,DS,S,S]
				startInclusive = destIndex + length;
				stopExclusive = srcIndex + length;
			}
		}
		for (int i = startInclusive * valueSize; i < (stopExclusive * valueSize); i++) {
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
	public void insertGap(int index, int length, Caller caller) {
		int lastNeededIndex = Math.max(lastUsedIndex + length, index + length);
		if ((lastNeededIndex + 1) > capacity) {
			long start = 0;
			if (StatisticsDBTest.SUBBENCHMARKS) {
				start = System.nanoTime();
			}
			byte[] target = createExtendedArray(lastNeededIndex + 1, valueSize);
			if (StatisticsDBTest.SUBBENCHMARKS) {
				subbenchmark(SubbenchmarkType.ALLOC, caller, System.nanoTime() - start);
				start = System.nanoTime();
			}
			// Don't use copy() for the left part because it would invalidate lastUsedIndex
			System.arraycopy(array, 0, target, 0, index * valueSize);
			if (StatisticsDBTest.SUBBENCHMARKS) {
				subbenchmark(SubbenchmarkType.ARRAYCOPY, caller, System.nanoTime() - start);
			}
			copy(index, index + length, (lastUsedIndex - index) + 1, target, caller);
			array = target;
		} else {
			move(index, index + length, (lastUsedIndex - index) + 1, caller);
		}
		capacity = array.length / valueSize;
	}

	/**
	 * Extends the internal array, ensuring at least <code>minCapacity</code> fields can be hold. The smallest multiple
	 * of the given extensionLength will be added to the capacity. If the minimum new capacity is the same as the
	 * current capacity, no additional capacity is added.
	 *
	 * Don't forget to manually update {@link #capacity} afterwards.
	 *
	 * @param minCapacity
	 * @param newValueSize
	 * @return
	 */
	private byte[] createExtendedArray(int minCapacity, int newValueSize) {
		int additionalCapacity = 0;
		if (minCapacity > capacity) {
			additionalCapacity = (((minCapacity - capacity) / extensionLength) + 1) * extensionLength;
		}
		return new byte[(capacity + additionalCapacity) * newValueSize];
	}

	/**
	 * Finds the index of the last value that is not equal to zero. Starting from the given index, the array is searched
	 * to the left direction as far as possible, and then to the right (again from the given position).
	 *
	 * @param startIndex
	 *            The first index that will be examined and used as starting position.
	 * @return Index of the last value != 0. Returns -1 if no values are used.
	 */
	// Might be useful for non-consecutive implementations
	@Deprecated
	protected int findLastUsedIndex(int startIndex) {
		int i = startIndex;
		// TODO: Finding the first value != 0 to the right does not mean thats the last value
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

	private int findLastUsedIndex(byte[] array) {
		for (int i = array.length - 1; (i >= 0) && (i < array.length); i--) {
			if (array[i] != 0) {
				return i / valueSize;
			}
		}
		return -1;
	}

	public int getLastUsedIndex() {
		return lastUsedIndex;
	}

	public byte[] getData() {
		byte[] data = new byte[(lastUsedIndex + 1) * valueSize];
		copy(0, 0, (lastUsedIndex + 1), data, null);
		return data;
	}

	@Override
	public String toString() {
		return array == null ? "[]" : Arrays.toString(array);
	}

	private static enum SubbenchmarkType {
		ARRAYCOPY,
		ALLOC
	}

	private void subbenchmark(SubbenchmarkType type, Caller caller, long time) {
		if (caller == Caller.NEXT) {
			if (type == SubbenchmarkType.ARRAYCOPY) {
				SubbenchmarkManager.getInstance().addTime(SubbenchmarkManager.SUBBENCHMARK_TASK.RLE_NEXT_ARRAYCOPY,
						time);
			} else if (type == SubbenchmarkType.ALLOC) {
				SubbenchmarkManager.getInstance().addTime(SubbenchmarkManager.SUBBENCHMARK_TASK.RLE_NEXT_ALLOC,
						time);
			}
		} else if (caller == Caller.RELEASE) {
			if (type == SubbenchmarkType.ARRAYCOPY) {
				SubbenchmarkManager.getInstance().addTime(SubbenchmarkManager.SUBBENCHMARK_TASK.RLE_RELEASE_ARRAYCOPY,
						time);
			} else if (type == SubbenchmarkType.ALLOC) {
				SubbenchmarkManager.getInstance().addTime(SubbenchmarkManager.SUBBENCHMARK_TASK.RLE_RELEASE_ALLOC,
						time);
			}
		}
	}

}
