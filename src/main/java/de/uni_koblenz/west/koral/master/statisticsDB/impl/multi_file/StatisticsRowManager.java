package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.MultiFileGraphStatisticsDatabase.ResourceType;

public class StatisticsRowManager {

	/**
	 * How many bytes are used for the data in the main/index file. This space is used for either file offset or
	 * occurences values.
	 */
	private static final int ROW_DATA_LENGTH = 8;

	/**
	 * How many bits are used for the column that describes how many bytes are used per occurence value.
	 */
	private static final int VALUE_LENGTH_COLUMN_BITLENGTH = 3;

	static enum PositionEncoding {
		BITMAP, LIST
	}

	/**
	 * How many bits are used for the column that describes how many occurence values exist.
	 */
	private final int positionCountBitLength;

	/**
	 * How many bytes are used for each position bitmap.
	 */
	private final int positionBitmapLength;

	/**
	 * How many bytes are used for metadata per row.
	 */
	private final int metadataLength;

	/**
	 * Length in bytes of each row in the main/index file.
	 */
	private final int mainfileRowLength;

	private final int numberOfChunks;

	private byte[] row;

	private boolean dataExternal;

	private long metadataBits;

	private long extraFileRowId;

	private int positionCount;

	private byte bytesPerValue;

	private PositionEncoding positionEncoding;

	private int positionLength;

	private int dataLength;

	private byte[] dataBytes;

	/**
	 * The index of the dataBytes array where the column that will be updated is found. Only used in update operations.
	 */
	private int bitmapEntryArrayIndex;

	/**
	 * The position in the byte referenced by {@link #bitmapEntryArrayIndex}, where the bit for the column that will be
	 * updated is found. Only used in update operations.
	 */
	private byte bitmapEntryOffset;

	public StatisticsRowManager(int numberOfChunks) {
		this.numberOfChunks = numberOfChunks;

		// -1 Offset for using zero as well
		int maxPositionCount = (3 * numberOfChunks) - 1;
		positionCountBitLength = 32 - Integer.numberOfLeadingZeros(maxPositionCount);
		metadataLength = (int) Math.ceil((positionCountBitLength + VALUE_LENGTH_COLUMN_BITLENGTH) / 8);
		positionBitmapLength = (int) Math.ceil((3 * numberOfChunks) / 8.0);
		mainfileRowLength = metadataLength + ROW_DATA_LENGTH;
	}

	int getMainFileRowLength() {
		return mainfileRowLength;
	}

	boolean isDataExternal() {
		return dataExternal;
	}

	long getExternalFileRowId() {
		return extraFileRowId;
	}

	int getDataLength() {
		return dataLength;
	}

	long getFileId() {
		return metadataBits;
	}

	byte[] getRow() {
		return row;
	}

	byte[] getDataBytes() {
		return dataBytes;
	}

	boolean isTooLongForMain() {
		return (metadataLength + dataBytes.length) > mainfileRowLength;
	}

	boolean load(byte[] row) {
		this.row = row;
		// Read metadata
		positionCount = extractPositionCount();
		bytesPerValue = extractBytesPerValue();

		positionEncoding = optimalPositionEncoding();
		if (positionEncoding == PositionEncoding.BITMAP) {
			positionLength = positionBitmapLength;
		} else if (positionEncoding == PositionEncoding.LIST) {
			positionLength = positionCount;
		}

		dataLength = (positionCount * bytesPerValue) + positionLength;

		if (dataLength <= ROW_DATA_LENGTH) {
			// Values are here
			dataExternal = false;
			dataBytes = new byte[dataLength];
			System.arraycopy(row, metadataLength, dataBytes, 0, dataLength);
			extraFileRowId = -1;
		} else {
			dataExternal = true;
			metadataBits = variableBytes2Long(row, 0, metadataLength);
			extraFileRowId = variableBytes2Long(row, metadataLength, ROW_DATA_LENGTH);
			dataBytes = null;
		}

		// Reset member variables that were not overwritten already
		bitmapEntryArrayIndex = -1;
		bitmapEntryOffset = -1;

		return dataExternal;

	}

	void loadExternalRow(byte[] dataBytes) {
		this.dataBytes = dataBytes;
	}

	byte[] create(ResourceType resourceType, int chunk) {
		row = new byte[mainfileRowLength];
		// Position count and bytesPerValue are already zero (-> one)
		// Set first entry of position list
		row[metadataLength] = (byte) (getColumnNumber(resourceType, chunk));
		// Occurs once (until now)
		row[metadataLength + 1] = 1;
		return row;
	}

	void mergeDataBytesIntoRow() {

	}

	void updateRowExtraOffset(long newOffset) {

	}

	/**
	 * Updates the loaded row by incrementing a occurence specified by resourceType and chunk. The {@link #row} is only
	 * updated with the meta data and otherwise unchanged, while {@link #dataBytes} contains the new occurence data.
	 *
	 * @param resourceType
	 *            The type of the resource that is to be incremented
	 * @param chunk
	 *            The chunk in which the resource occured
	 */
	void incrementOccurence(ResourceType resourceType, int chunk) {
		int columnNumber = getColumnNumber(resourceType, chunk);
		bitmapEntryArrayIndex = columnNumber / 8;
		bitmapEntryOffset = (byte) (columnNumber % 8);
		int currentOccurenceValueIndex = getOccurenceValueIndex(columnNumber);
		if (currentOccurenceValueIndex < 0) {
			// Resource never occured at this column position yet
			if (optimalPositionEncoding() != positionEncoding) {
				// Data bytes have to be completely rewritten with new encoding
				long[] oldOccurences = decodeOccurenceData();
				positionEncoding = optimalPositionEncoding();
				encodeOccurenceData(oldOccurences);
			} else {
				// Insert value into dataBytes at the correct position
				if (positionEncoding == PositionEncoding.BITMAP) {
					// Set bitmap bit
					setBitmapBit(columnNumber);
					currentOccurenceValueIndex = getValueIndexOfColumn(columnNumber);
					// Prepare new data bytes that will need space for one more value
					dataBytes = extendArray(dataBytes, bytesPerValue);
					// Make space for new occurence value
					int currentOccurenceOffset = metadataLength + (currentOccurenceValueIndex * bytesPerValue);
					moveBytesRight(dataBytes, currentOccurenceOffset, positionCount - currentOccurenceValueIndex,
							bytesPerValue);
					// Insert new occurence
					writeLongIntoBytes(1, dataBytes, currentOccurenceOffset, bytesPerValue);
				} else if (positionEncoding == PositionEncoding.LIST) {
					// Prepare new data bytes that will have space for one more position byte and one more value
					dataBytes = extendArray(dataBytes, 1 + bytesPerValue);
					// Make space for new position byte
					moveBytesRight(dataBytes, positionLength, positionCount, 1);
					// Extend position list with new position
					dataBytes[positionLength] = (byte) columnNumber;
					positionLength++;
					// Extend value list with new value
					dataBytes[positionLength + positionCount] = 1;
				}
				positionCount++;
				// Write new position count into meta column
				updatePositionCount();
			}
		} else {
			// Resource column has entry that will be updated now
			// Get occurence value
			long occurences = variableBytes2Long(dataBytes,
					positionLength + (currentOccurenceValueIndex * bytesPerValue), bytesPerValue);
			occurences++;
			// Check if more bytes are needed now for values
			if (occurences >= (1L << (bytesPerValue * Byte.SIZE))) {
				// Extract old occurence values
				long[] oldOccurences = decodeOccurenceData();
				// Update current occurence
				oldOccurences[currentOccurenceValueIndex] += 1;
				bytesPerValue++;
				// Rewrite values
				dataBytes = extendArray(dataBytes, positionCount);
				for (int i = 0; i < positionCount; i++) {
					writeLongIntoBytes(oldOccurences[i], dataBytes, positionLength + (i * bytesPerValue),
							bytesPerValue);
				}
				// Increase bytesPerValue column
				updateBytesPerValue();
			} else {
				// Update data bytes/the one occurence value
				writeLongIntoBytes(occurences, dataBytes, currentOccurenceValueIndex * bytesPerValue, bytesPerValue);
			}

		}
	}

	/**
	 * Decodes the data bytes of a row and returns the positioned occurence values.
	 *
	 * @param dataBytes
	 *            The data bytes of a row
	 * @return An array with the length of all columns, with occurence values at their corresponding columns position,
	 *         e.g. [0, 1, 1, 0, 0, 0, 7, 0, 2]
	 */
	long[] decodeOccurenceData() {
		long[] occurenceValues = new long[3 * numberOfChunks];
		int[] positions = extractPositions();
		for (int i = 0; i < positions.length; i++) {
			long occurences = variableBytes2Long(dataBytes, positionLength + i, bytesPerValue);
			occurenceValues[positions[i]] = occurences;
		}
		return occurenceValues;
	}

	/**
	 * Encodes occurence data into the data byte format, either with a bitmap or list encoding (as specified).
	 * Overwrites {@link #dataBytes}.
	 *
	 * @param occurences
	 *            An array that maps each column number to its occurences, like the output of
	 *            {@link #decodeOccurenceData(byte[], PositionEncoding, int, byte)}.
	 * @return A data byte array with the length only as long as needed
	 */
	private void encodeOccurenceData(long[] occurences) {
		if (positionEncoding == PositionEncoding.BITMAP) {
			positionLength = positionBitmapLength;
		} else if (positionEncoding == PositionEncoding.LIST) {
			positionLength = positionCount;
		}
		dataBytes = new byte[positionLength + (positionCount * bytesPerValue)];
		for (int i = 0; i < occurences.length; i++) {
			if (occurences[i] != 0) {
				if (positionEncoding == PositionEncoding.BITMAP) {
					setBitmapBit(i);
				} else if (positionEncoding == PositionEncoding.LIST) {
					dataBytes[i] = (byte) i;
				}
				writeLongIntoBytes(occurences[i], dataBytes, positionLength + (i * bytesPerValue), bytesPerValue);
			}
		}
	}

	private int getColumnNumber(ResourceType resourceType, int chunk) {
		return (resourceType.position() * numberOfChunks) + chunk;
	}

	private int extractPositionCount() {
		return (int) metadataBits >>> ((metadataLength * 8) - positionCountBitLength);
	}

	/**
	 * Replaces the old positionCount value in {@link #row} with the given new one.
	 *
	 */
	private void updatePositionCount() {
		int postionCountOffset = (metadataLength * 8) - positionCountBitLength;
		// This bitfilter will be zero everywhere, except for the bytesPerValue bits, to remove the previous position
		// count bits.
		long bitFilter = (1L << postionCountOffset) - 1;
		// Remove old position count
		metadataBits &= bitFilter;
		// Insert new position count
		metadataBits |= positionCount << postionCountOffset;
		writeLongIntoBytes(metadataBits, row, 0, metadataLength);
	}

	private byte extractBytesPerValue() {
		// This works only if the bits are all in the same (last) byte
		assert VALUE_LENGTH_COLUMN_BITLENGTH <= 8;
		return (byte) (row[metadataLength - 1] & ((1 << VALUE_LENGTH_COLUMN_BITLENGTH) - 1));
	}

	/**
	 * Replaces the old bytesPerValue value in {@link #row} with the given new one.
	 *
	 */
	private void updateBytesPerValue() {
		// This works only if the bits are all in the same (last) byte
		assert VALUE_LENGTH_COLUMN_BITLENGTH <= 8;
		// Remove old value
		row[metadataLength - 1] &= (1 << 31) >> (31 - VALUE_LENGTH_COLUMN_BITLENGTH);
		// Insert new value
		row[metadataLength] |= bytesPerValue;
	}

	/**
	 * Detects the optimal position encoding by comparing each option's needed bytes.
	 *
	 * @return
	 */
	private PositionEncoding optimalPositionEncoding() {
		if (positionCount <= positionBitmapLength) {
			return PositionEncoding.LIST;
		} else {
			return PositionEncoding.BITMAP;
		}
	}

	/**
	 * Extracts all positions/column numbers which have an occurence value >1, i.e. have an associated value in the data
	 * bytes.
	 *
	 * @return An array of all columns with occurences, sorted by the order of their corresponding values (second
	 *         element of this maps to second occurence value). The length is positionCount. Example: [1,1,2,1,5,3,1]
	 */
	private int[] extractPositions() {
		int[] positions = new int[positionLength];
		if (positionEncoding == PositionEncoding.BITMAP) {
			for (int i = 0; i < positionLength; i++) {
				positions[i] = dataBytes[i];
			}
		} else if (positionEncoding == PositionEncoding.BITMAP) {
			long positionBitmap = variableBytes2Long(dataBytes, 0, positionLength);
			// Stores the position in the bitmap i.e. the index where the value will be stored
			int position = 0;
			// Stores the index of the current occurence value
			int valueIndex = 0;
			// Iterate over each bit in the position bitmap with a filter index
			for (long filterIndex = 1L << (positionBitmapLength * 8); // Start value is 1000...0
					filterIndex > 1; // Continue until filter index is at the last bit
					filterIndex >>>= 1 // Move the 1 one to the right each step
					) {
				if ((filterIndex & positionBitmap) > 0) {
					positions[valueIndex] = position;
					valueIndex++;
				}
				position++;
			}
		}
		return positions;
	}

	/**
	 * Returns the bit of the position bitmap at the specified column.
	 *
	 * @param columnNumber
	 *            Which column will be read
	 * @return The bit as byte
	 */
	private byte getBitmapBit(int columnNumber) {
		return (byte) (dataBytes[bitmapEntryArrayIndex] & (1 << (8 - bitmapEntryOffset - 1)));
	}

	/**
	 * Sets the bit at columnNumber to 1.
	 *
	 * @param columnNumber
	 *            Which column to update
	 */
	private void setBitmapBit(int columnNumber) {
		// dataBytes might be too long for conversion to long, so we have to change the array directly
		dataBytes[bitmapEntryArrayIndex] |= 1 << (8 - bitmapEntryOffset - 1);
	}

	/**
	 * Returns the index of the value that corresponds to the specified columnNumber, based on a bitmap given in
	 * dataBytes.
	 *
	 * @param columnNumber
	 *            The column of the wanted occurence value
	 * @return
	 */
	private int getValueIndexOfColumn(int columnNumber) {
		return countOnesUntil(dataBytes, bitmapEntryArrayIndex, bitmapEntryOffset);
	}

	/**
	 * Converts a part of a byte array to a long value.
	 *
	 * @param bytes
	 *            The byte array
	 * @param startIndex
	 *            The first byte that will be included
	 * @param length
	 *            How many bytes will be included
	 * @return
	 */

	/**
	 * Finds the index of the occurence value corresponding to the given position column. {@link #bitmapEntryArrayIndex}
	 * and {@link #bitmapEntryOffset} must be set already before calling this method.
	 *
	 * @param columnNumber
	 * @return
	 */
	private int getOccurenceValueIndex(int columnNumber) {
		if (positionEncoding == PositionEncoding.BITMAP) {
			byte bitmapEntry = (byte) (dataBytes[bitmapEntryArrayIndex] & (1 << (8 - bitmapEntryOffset - 1)));
			// If that exists, find position of corresponding value
			if (bitmapEntry != 0) {
				return countOnesUntil(dataBytes, bitmapEntryArrayIndex, bitmapEntryOffset);
			}
		} else if (positionEncoding == PositionEncoding.LIST) {
			for (int i = 0; i < positionLength; i++) {
				if (dataBytes[i] == columnNumber) {
					return i;
				}
			}
		}
		return -1;
	}

	private static long variableBytes2Long(byte[] bytes, int startIndex, int length) {
		long result = 0L;
		for (int i = 0; i < length; i++) {
			// Shift byte based on its position. -1 because we dont need to shift the last byte
			result += ((long) bytes[startIndex + i]) << (8 * (length - 1 - i));
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
	private static void writeLongIntoBytes(long number, byte[] bytes, int startIndex, int length) {
		for (int i = 0; i < length; i++) {
			// Update bytes in reverse order, and shift the necessary byte of the long to the last 8 bits
			bytes[(startIndex + length) - i] = (byte) (0xFF & (number >>> (i * 8)));
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
	private static int countOnesUntil(byte[] array, int lastIndex, byte lastBit) {
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
	private static void moveBytesRight(byte[] bytes, int startIndex, int length, int offset) {
		// Iterate interval in reverse order
		for (int i = startIndex + length; i >= startIndex; i--) {
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
	private static byte[] extendArray(byte[] array, int additionalFields) {
		byte[] newArray = new byte[array.length + additionalFields];
		System.arraycopy(array, 0, newArray, 0, array.length);
		return newArray;
	}
}
