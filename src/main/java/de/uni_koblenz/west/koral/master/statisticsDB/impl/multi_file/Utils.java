package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

/**
 * Bundles different low-level frequently-used utility functions.
 *
 * @author Philipp Töws
 *
 */
public class Utils {

	private Utils() {}

	/**
	 * Converts a part of a byte array into a long value.
	 *
	 * @param bytes
	 *            A series of bytes
	 * @param startIndex
	 *            The first byte that will be incorporated
	 * @param length
	 *            How many bytes will be incorporated
	 * @return
	 */
	public static long variableBytes2Long(byte[] bytes, int startIndex, int length) {
		long result = 0L;
		for (int i = 0; i < length; i++) {
			byte currentByte = bytes[startIndex + i];
			// Shift byte based on its position. -1 because we dont need to shift the last byte.
			// Convert to unsigned long to prevent left bits being filled up with ones
			result += (Byte.toUnsignedLong(currentByte)) << (8 * (length - 1 - i));
		}
		return result;
	}

	/**
	 * Writes a long value into a part of a byte array.
	 *
	 * @param bytes
	 *            The byte array where the long will be inserted
	 * @param startIndex
	 *            The first byte that will be overwritten
	 * @param length
	 *            How many bytes will be overwritten
	 */
	public static void writeLongIntoBytes(long number, byte[] bytes, int startIndex, int length) {
		for (int i = 0; i < length; i++) {
			// Update bytes in reverse order, and shift the necessary byte of the long to the last 8 bits
			bytes[((startIndex + length) - 1) - i] = (byte) (0xFF & (number >>> (i * 8)));
		}
	}

	/**
	 * Counts binary ones in a series of bytes up to a given position.
	 *
	 * @param array
	 *            Series of bytes
	 * @param lastIndex
	 *            The last byte that will be regarded
	 * @param lastBit
	 *            The last bit of that last byte that will be regarded, it will not be counted. Indexing starts at zero.
	 * @return Amount of ones
	 */
	public static int countOnesUntil(byte[] array, int lastIndex, byte lastBit) {
		int ones = 0;
		for (int i = 0; i <= lastIndex; i++) {
			for (byte j = 0; j < 8; j++) {
				if ((i == lastIndex) && (j == lastBit)) {
					break;
				}
				// The position number j is converted to a byte with a one at j
				if ((array[i] & (1 << (8 - 1 - j))) > 0) {
					ones++;
				}
			}
		}
		return ones;
	}

	/**
	 * Shifts a part of the bytes to the right. Bytes to the right of the specified interval will be overwritten. Moved
	 * parts will *not* be overwritten.
	 *
	 * @param bytes
	 *            The bytes that will be changed
	 * @param startIndex
	 *            The first byte that will be moved
	 * @param length
	 *            How many bytes will be moved
	 * @param offset
	 *            By how many indexes each byte will move. Must be >=0
	 */
	public static void moveBytesRight(byte[] bytes, int startIndex, int length, int offset) {
		// Iterate interval in reverse order
		for (int i = (startIndex + length) - 1; i >= startIndex; i--) {
			bytes[i + offset] = bytes[i];
		}
	}

	/**
	 * Clones an array and extends it by additionalFields.
	 *
	 * @param array
	 *            The array to be extended
	 * @param additionalFields
	 *            The additional value that is added to the size
	 * @return A new array with the content from array until array.length, but with length array.length +
	 *         additionalFields
	 */
	public static byte[] extendArray(byte[] array, int additionalFields) {
		byte[] newArray = new byte[array.length + additionalFields];
		System.arraycopy(array, 0, newArray, 0, array.length);
		return newArray;
	}

	/**
	 * Checks if every field of the array is equal to zero.
	 *
	 * @param array
	 *            A byte array with arbitrary length
	 * @return True if every entry is equal to zero, false if at least one entry is different from zero.
	 */
	public static boolean isArrayZero(byte[] array) {
		for (int i = 0; i < array.length; i++) {
			if (array[i] != 0) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Checks if every field of the array is equal to zero.
	 *
	 * @param array
	 *            A long array with arbitrary length
	 * @return True if every entry is equal to zero, false if at least one entry is different from zero.
	 */
	public static boolean isArrayZero(long[] array) {
		for (int i = 0; i < array.length; i++) {
			if (array[i] != 0) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Calculates how many bytes are needed to represent the given value either signed or unsigned. Note that every
	 * negative number stored as unsigned number will need all of the possible 8 bytes.
	 *
	 * @param value
	 *            The value that should be represented, can be positive or negative in both cases
	 * @param signed
	 *            Whether the bytes representation should be signed or not
	 * @return How many bytes are at minimum needed to represent the given value as a signed number
	 */
	public static int neededBytesForValue(long value, boolean signed) {
		if (value == 0) {
			return 1;
		}
		if (signed && (value < 0)) {
			// There is no Long.numberOfLeadingOnes(), so transfer negative case to positive one
			value ^= -1;
		}
		int neededBits = ((Long.BYTES * Byte.SIZE) - Long.numberOfLeadingZeros(value));
		if (signed) {
			// For sign bit
			neededBits++;
		}
		return (int) Math.ceil(neededBits / 8.0);
	}
}
