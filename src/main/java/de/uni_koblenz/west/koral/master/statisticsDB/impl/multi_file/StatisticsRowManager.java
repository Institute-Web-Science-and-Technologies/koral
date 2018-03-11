package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import java.util.Arrays;

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

	private final int positionBitmapBitLength;

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

//	/**
//	 * The index of the dataBytes array where the column that will be updated is found. Only used in update operations.
//	 */
//	private int bitmapEntryArrayIndex;
//
//	/**
//	 * The position in the byte referenced by {@link #bitmapEntryArrayIndex}, where the bit for the column that will be
//	 * updated is found. Only used in update operations.
//	 */
//	private byte bitmapEntryOffset;

	public StatisticsRowManager(int numberOfChunks) {
		this.numberOfChunks = numberOfChunks;

		// -1 Offset for using zero as well
		int maxPositionCount = (3 * numberOfChunks) - 1;
		positionCountBitLength = 32 - Integer.numberOfLeadingZeros(maxPositionCount);
		metadataLength = (int) Math.ceil((positionCountBitLength + VALUE_LENGTH_COLUMN_BITLENGTH) / 8.0);
		positionBitmapBitLength = (3 * numberOfChunks);
		positionBitmapLength = (int) Math.ceil(positionBitmapBitLength / 8.0);
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

	/**
	 * Initializes the class with a new row. All internal values and states are reset or overwritten with extracted and
	 * decoded information of this row. Executing this method is like creating a new instance, but more performant.
	 * Final fields like row length that apply to each row are obviously untouched.
	 *
	 * @param row
	 *            A read row from the main file.
	 * @return True if the row refers to an extra file for the position info, false if the position info is already
	 *         included in the row.
	 */
	boolean load(byte[] row) {
		this.row = row;
		// Read metadata
		metadataBits = variableBytes2Long(row, 0, metadataLength);
		positionCount = extractPositionCount();
		bytesPerValue = extractBytesPerValue();

		positionEncoding = optimalPositionEncoding(positionCount);
		System.out
				.println("Loaded position encoding " + positionEncoding + " because of positionCount " + positionCount);
		positionLength = getPositionLength(positionEncoding);

		dataLength = (positionCount * bytesPerValue) + positionLength;

		if (dataLength <= ROW_DATA_LENGTH) {
			// Values are here
			dataExternal = false;
			dataBytes = new byte[dataLength];
			System.arraycopy(row, metadataLength, dataBytes, 0, dataLength);
			extraFileRowId = -1;
		} else {
			dataExternal = true;
			extraFileRowId = variableBytes2Long(row, metadataLength, ROW_DATA_LENGTH);
			dataBytes = null;
		}

		// Reset member variables that were not overwritten already
//		bitmapEntryArrayIndex = -1;
//		bitmapEntryOffset = -1;

		return dataExternal;

	}

	/**
	 * Replaces the internal dataBytes array with the given one. This is needed if the actual position info is located
	 * in an extra file, which must be read externally, and then given to this class by this method.
	 *
	 * @param dataBytes
	 *            The new data bytes
	 */
	void loadExternalRow(byte[] dataBytes) {
		this.dataBytes = dataBytes;
	}

	/**
	 * Creates and returns a row for a resource that occured for the first time. The position count is one, the value of
	 * the bytes-per-value column is one, and the position is encoded as list.
	 *
	 * @param resourceType
	 *            The type of the resource that is to be incremented
	 * @param chunk
	 *            The chunk in which the resource occured
	 * @return The new row as byte array, with a length of the row length in the main file.
	 */
	byte[] create(ResourceType resourceType, int chunk) {
		row = new byte[mainfileRowLength];
		// Position count and bytesPerValue are already zero (-> one)
		// Set first entry of position list
		row[metadataLength] = (byte) (getColumnNumber(resourceType, chunk));
		// Occurs once (until now)
		row[metadataLength + 1] = 1;
		return row;
	}

	/**
	 * Writes internal dataBytes into internal row.
	 */
	void mergeDataBytesIntoRow() {
		System.arraycopy(dataBytes, 0, row, metadataLength, dataBytes.length);
	}

	/**
	 * Writes new extra file row id into the data bytes of the internal row.
	 *
	 * @param newRowId
	 *            Thw new row id
	 */
	void updateRowExtraOffset(long newRowId) {
		writeLongIntoBytes(newRowId, row, metadataLength, ROW_DATA_LENGTH);
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
		System.out.println("Column Number " + columnNumber);
		int currentOccurenceValueIndex = getOccurenceValueIndex(columnNumber);
		System.out.println("Occurence Value Index: " + currentOccurenceValueIndex);
		if (currentOccurenceValueIndex < 0) {
			// Resource never occured at this column position yet
			if (optimalPositionEncoding(positionCount + 1) != positionEncoding) {
				// Data bytes have to be completely rewritten with new encoding
				long[] oldOccurences = decodeOccurenceData();
				// Add new occurence
				oldOccurences[columnNumber] = 1;
				positionCount++;
				positionEncoding = optimalPositionEncoding(positionCount);
				System.out.println("Switched position encoding to " + positionEncoding);
				encodeOccurenceData(oldOccurences);
			} else {
				// Insert value into dataBytes at the correct position
				if (positionEncoding == PositionEncoding.BITMAP) {
					// Set bitmap bit
					setBitmapBit(columnNumber);
					currentOccurenceValueIndex = getValueIndexOfColumn(columnNumber);
					System.out.println("Occurence Value Index: " + currentOccurenceValueIndex);
					// Prepare new data bytes that will need space for one more value
					dataBytes = extendArray(dataBytes, bytesPerValue);
					// Make space for new occurence value
					int currentOccurenceOffset = positionLength + (currentOccurenceValueIndex * bytesPerValue);
					System.out.println("Occurence Offset: " + currentOccurenceOffset);
					// If the new value must be inserted somewhere inbetween/not at the last position...
					if (currentOccurenceOffset != (dataBytes.length - 1)) {
						// Make space by moving rightern bytes
						moveBytesRight(dataBytes, currentOccurenceOffset, positionCount - currentOccurenceValueIndex,
								bytesPerValue);
					}
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
			}
			// Write new position count into meta column
			updatePositionCount();
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
				writeLongIntoBytes(occurences, dataBytes, positionLength + (currentOccurenceValueIndex * bytesPerValue),
						bytesPerValue);
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
		System.out.println("Positions: " + Arrays.toString(positions));
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
		System.out.println("Encoding occurences: " + Arrays.toString(occurences));
		positionLength = getPositionLength(positionEncoding);
		dataBytes = new byte[positionLength + (positionCount * bytesPerValue)];
		System.out.println("New DataBytes length: " + dataBytes.length);
		// Counts the positions != 0
		int positionIndex = 0;
		for (int i = 0; i < occurences.length; i++) {
			if (occurences[i] != 0) {
				System.out.println("Found occurence at " + i + ": " + occurences[i]);
				if (positionEncoding == PositionEncoding.BITMAP) {
					setBitmapBit(i);
				} else if (positionEncoding == PositionEncoding.LIST) {
					dataBytes[positionIndex] = (byte) i;
				}
				writeLongIntoBytes(occurences[i], dataBytes, positionLength + (positionIndex * bytesPerValue),
						bytesPerValue);
				System.out.println("Updated dataBytes: " + Arrays.toString(dataBytes));
				positionIndex++;
			}
		}
	}

	/**
	 * @param resourceType
	 *            As which type of resource the resource occured
	 * @param chunk
	 *            In which chunk the resource occured
	 * @return In which column the resource would be stored
	 */
	private int getColumnNumber(ResourceType resourceType, int chunk) {
		return (resourceType.position() * numberOfChunks) + chunk;
	}

	/**
	 * @return The value of the position count column in the current {@link #row}. The offset of -1 is removed.
	 */
	private int extractPositionCount() {
		return ((int) metadataBits >>> ((metadataLength * 8) - positionCountBitLength)) + 1;
	}

	/**
	 * Replaces the old positionCount value in {@link #row} with the given new one. The offset of -1 is applied
	 * beforehand.
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
		metadataBits |= (positionCount - 1) << postionCountOffset;
		writeLongIntoBytes(metadataBits, row, 0, metadataLength);
	}

	/**
	 * @return The value of the bytes-per-value column in the current {@link #row}. The offset of -1 is removed.
	 */
	private byte extractBytesPerValue() {
		// This works only if the bits are all in the same (last) byte
		assert VALUE_LENGTH_COLUMN_BITLENGTH <= 8;
		return (byte) ((row[metadataLength - 1] & ((1 << VALUE_LENGTH_COLUMN_BITLENGTH) - 1)) + 1);
	}

	/**
	 * Replaces the old bytesPerValue value in {@link #row} with the given new one. The offset of -1 is applied
	 * beforehand.
	 *
	 */
	private void updateBytesPerValue() {
		// This works only if the bits are all in the same (last) byte
		assert VALUE_LENGTH_COLUMN_BITLENGTH <= 8;
		// Remove old value
		row[metadataLength - 1] &= (1 << 31) >> (31 - VALUE_LENGTH_COLUMN_BITLENGTH);
		// Insert new value
		row[metadataLength] |= bytesPerValue - 1;
	}

	/**
	 * Detects the optimal position encoding by comparing each option's needed bytes.
	 *
	 * @return
	 */
	private PositionEncoding optimalPositionEncoding(int positionCount) {
		if (positionCount <= positionBitmapLength) {
			return PositionEncoding.LIST;
		} else {
			return PositionEncoding.BITMAP;
		}
	}

	/**
	 * Computes the length of a specified position encoding, using internal variables {@link #positionBitmapLength} or
	 * {@link #positionCount}.
	 *
	 * @param positionEncoding
	 *            The encoding type
	 * @return The amount of bytes that would be needed for the given encoding and the classes internal results
	 */
	private int getPositionLength(PositionEncoding positionEncoding) {
		if (positionEncoding == PositionEncoding.BITMAP) {
			return positionBitmapLength;
		} else if (positionEncoding == PositionEncoding.LIST) {
			return positionCount;
		}
		return 0;
	}

	/**
	 * Extracts all positions/column numbers which have an occurence value >1, i.e. have an associated value in the data
	 * bytes.
	 *
	 * @return An array of all columns with occurences, sorted by the order of their corresponding values (second
	 *         element of this maps to second occurence value). The length is positionCount. Example: [1,1,2,1,5,3,1]
	 */
	private int[] extractPositions() {
		int[] positions = new int[positionCount];
		if (positionEncoding == PositionEncoding.LIST) {
			for (int i = 0; i < positionLength; i++) {
				positions[i] = dataBytes[i];
			}
		} else if (positionEncoding == PositionEncoding.BITMAP) {
			long positionBitmap = variableBytes2Long(dataBytes, 0, positionLength);
			System.out.println("Position bitmap: " + String.format("0x%08X", positionBitmap));
			// Stores the position in the bitmap i.e. the index where the value will be stored
			// We need an offset, because not every bit is used (bitmap size was rounded up to bytes)
			int position = 0;
			// Stores the index of the current occurence value
			int valueIndex = 0;
			// Iterate over each bit in the position bitmap with a filter index
			for (long filterIndex = 1L << (positionBitmapBitLength - 1); // Start value is 1000...0
					filterIndex >= 1; // Continue until filter index is at the last bit
					filterIndex >>>= 1 // Move the 1 one to the right each step
					) {
				System.out.println("FilterIndex: " + String.format("0x%08X", filterIndex));
				if ((filterIndex & positionBitmap) > 0) {
					System.out.println("Position " + valueIndex + " is " + position);
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
//	private byte getBitmapBit(int columnNumber) {
//		return (byte) (dataBytes[bitmapEntryArrayIndex] & (1 << (8 - bitmapEntryOffset - 1)));
//	}

	/**
	 * Sets the bit at columnNumber to 1.
	 *
	 * @param columnNumber
	 *            Which column to update
	 */
	private void setBitmapBit(int columnNumber) {
		// dataBytes might be too long for conversion to long, so we have to change the array directly
		int positionBitmapIndex = getPositionBitmapIndex(columnNumber);
		int bitmapEntryArrayIndex = positionBitmapIndex / 8;
		byte bitmapEntryOffset = (byte) (positionBitmapIndex % 8);
		System.out.println("Setting bitmap bit at " + bitmapEntryArrayIndex + "/" + bitmapEntryOffset);
		dataBytes[bitmapEntryArrayIndex] |= 1 << (8 - bitmapEntryOffset - 1);
		System.out.println("Bit set: " + String.format("0x%08X", dataBytes[bitmapEntryArrayIndex]));
	}

	/**
	 * Adds an offset to the columnNumber to get the position of the corresponding bit in the position bitmap. The
	 * offset exists because the bitmap length was rounded up to bytes.
	 *
	 * @param columnNumber
	 * @return
	 */
	private int getPositionBitmapIndex(int columnNumber) {
		return columnNumber + ((positionBitmapLength * Byte.SIZE) - positionBitmapBitLength);
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
		int positionBitmapIndex = getPositionBitmapIndex(columnNumber);
		int bitmapEntryArrayIndex = positionBitmapIndex / 8;
		byte bitmapEntryOffset = (byte) (positionBitmapIndex % 8);
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
			int positionBitmapIndex = getPositionBitmapIndex(columnNumber);
			int bitmapEntryArrayIndex = positionBitmapIndex / 8;
			byte bitmapEntryOffset = (byte) (positionBitmapIndex % 8);
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
	private static long variableBytes2Long(byte[] bytes, int startIndex, int length) {
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
	private static void writeLongIntoBytes(long number, byte[] bytes, int startIndex, int length) {
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
		System.out.println("Bytes: " + Arrays.toString(bytes));
		System.out.println("Start: " + startIndex + ", length: " + length + ", offset: " + offset);
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
