package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import java.util.Arrays;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.MultiFileGraphStatisticsDatabase.ResourceType;

public class StatisticsRowManager {

	/**
	 * How many bytes are used for the data in the main/index file. This space is used for either file offset or
	 * occurences values. Note that some code has to be adapted if this value is set to something >8, because then
	 * converting more than 8 bytes to a long might overflow.
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
	 * How many bits are used for each position bitmap. Therefore this is the actual length of the bitmap.
	 */
	private final int positionBitmapBitLength;

	/**
	 * How many bytes are used for each position bitmap. This length is equal to {@link #positionBitmapBitLength} but
	 * rounded up to bytes.
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

	/**
	 * How many chunks where created, as specified in the constructor.
	 */
	private final int numberOfChunks;

	/**
	 * Contains the raw row bytes of the index/main file. In the update process, the data bytes are extracted (if they
	 * are located in the index row) and only merged in the ending. The metadata bits, though, are always updated on
	 * changes applied to {@link #metadataBits}.
	 */
	private byte[] row;

	/**
	 * True if it was recognized that the data bytes must be located in an extra file, based on calculations with the
	 * metadata bits.
	 */
	private boolean dataExternal;

	/**
	 * The extracted metadata bits containing left-aligned position count, a fill gap of zeroes and the right-aligned
	 * value of the bytes-per-value column. It is ensured that this value always represents the current state of the
	 * {@link #row}, and vice versa. The included position count and bytes per value numbers still have the -1 offset
	 * applied.
	 */
	private long metadataBits;

	/**
	 * Stores the row id of the extra file where the data bytes are located if, after analyzing the read row, it is
	 * concluded that they would be too long for the index/main file.
	 */
	private long extraFileRowId;

	/**
	 * The first column of each row, and the first part of the metadata bits: It describes in how many columns this
	 * resource appeared. A column is identified by resource type and chunk index. This variable contains the real
	 * value, without the offset of -1 existing in the raw {@link #metadataBits}, {@link #row} and index file.
	 */
	private int positionCount;

	/**
	 * The second column of each row, and the second part of the metadata bits: It describes how many bytes are needed
	 * to store the occurence values. This variable contains the real value, without the offset of -1 existing in the
	 * raw {@link #metadataBits}, {@link #row} and index file.
	 */
	private byte bytesPerValue;

	/**
	 * Describes how the positions of the current row are encoded.
	 */
	private PositionEncoding positionEncoding;

	/**
	 * How many bytes are needed for the position info in the current row.
	 */
	private int positionLength;

	/**
	 * How long the data bytes are supposed to be, based on the metadata bits. This value is used for reading a row in
	 * an extra file, but is *not* used or updated later on.
	 */
	private int dataLength;

	/**
	 * The second part of the row, basically the rest after the metadataBits. It contains the position info and the
	 * corresponding occurence values, encoded as in {@link #positionEncoding} described.
	 */
	private byte[] dataBytes;

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
	@SuppressWarnings("unused")
	boolean load(byte[] row) {
		this.row = row;
		// Read metadata
		metadataBits = Utils.variableBytes2Long(row, 0, metadataLength);
		positionCount = extractPositionCount();
		bytesPerValue = extractBytesPerValue();

		positionEncoding = optimalPositionEncoding(positionCount);
		Logger.log("Enc: " + positionEncoding + ", PC: " + positionCount);
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
			assert ROW_DATA_LENGTH <= 8 : "Long might overflow";
			extraFileRowId = Utils.variableBytes2Long(row, metadataLength, ROW_DATA_LENGTH);
			dataBytes = null;
		}

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
		Utils.writeLongIntoBytes(newRowId, row, metadataLength, ROW_DATA_LENGTH);
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
		int currentOccurenceValueIndex = getOccurenceValueIndex(columnNumber);
		Logger.log("Col: " + columnNumber + "; OVI: " + currentOccurenceValueIndex);
		if (currentOccurenceValueIndex < 0) {
			// Resource never occured at this column position yet
			if (optimalPositionEncoding(positionCount + 1) != positionEncoding) {
				// Data bytes have to be completely rewritten with new encoding
				long[] oldOccurences = decodeOccurenceData();
				// Add new occurence
				oldOccurences[columnNumber] = 1;
				Logger.log("Occurences now: " + Arrays.toString(oldOccurences));
				positionCount++;
				positionEncoding = optimalPositionEncoding(positionCount);
				Logger.log("Switched to " + positionEncoding);
				encodeOccurenceData(oldOccurences);
			} else {
				// Insert value into dataBytes at the correct position
				if (positionEncoding == PositionEncoding.BITMAP) {
					setBitmapBit(columnNumber);
					currentOccurenceValueIndex = getValueIndexOfColumn(columnNumber);
					// Prepare new data bytes that will need space for one more value
					dataBytes = Utils.extendArray(dataBytes, bytesPerValue);
					// Make space for new occurence value
					int currentOccurenceOffset = positionLength + (currentOccurenceValueIndex * bytesPerValue);
					// If the new value must be inserted somewhere inbetween/not at the last position...
					if (currentOccurenceOffset != (dataBytes.length - 1)) {
						// Make space by moving rightern bytes
						Utils.moveBytesRight(dataBytes, currentOccurenceOffset,
								positionCount - currentOccurenceValueIndex, bytesPerValue);
					}
					// Insert new occurence
					Utils.writeLongIntoBytes(1, dataBytes, currentOccurenceOffset, bytesPerValue);
				} else if (positionEncoding == PositionEncoding.LIST) {
					// Prepare new data bytes that will have space for one more position byte and one more value
					dataBytes = Utils.extendArray(dataBytes, 1 + bytesPerValue);
					// Make space for new position byte
					Utils.moveBytesRight(dataBytes, positionLength, positionCount * bytesPerValue, 1);
					// Extend position list with new position
					dataBytes[positionLength] = (byte) columnNumber;
					positionLength++;
					// Extend value list with new value
					dataBytes[positionLength + (positionCount * bytesPerValue) + (bytesPerValue - 1)] = 1;
				}
				positionCount++;
			}
			updatePositionCount();
		} else {
			// Resource column has entry that will be updated now
			// Get occurence value
			long occurences = Utils.variableBytes2Long(dataBytes,
					positionLength + (currentOccurenceValueIndex * bytesPerValue), bytesPerValue);
			occurences++;
			// Check if more bytes are needed now for values
			if (occurences >= (1L << (bytesPerValue * Byte.SIZE))) {
				// Extract old occurence values
				long[] oldOccurences = decodeOccurenceData();
				// Update current occurence
				oldOccurences[columnNumber] += 1;
				Logger.log("new OC: " + Arrays.toString(oldOccurences));
				bytesPerValue++;
				// Rewrite values
				dataBytes = Utils.extendArray(dataBytes, positionCount);
				int positionIndex = 0;
				for (int i = 0; i < oldOccurences.length; i++) {
					if (oldOccurences[i] != 0) {
						Utils.writeLongIntoBytes(oldOccurences[i], dataBytes,
								positionLength + (positionIndex * bytesPerValue), bytesPerValue);
						positionIndex++;
					}
				}
				Logger.log("DB: " + Arrays.toString(dataBytes));
				updateBytesPerValue();
			} else {
				// Update data bytes/the one occurence value
				Utils.writeLongIntoBytes(occurences, dataBytes,
						positionLength + (currentOccurenceValueIndex * bytesPerValue), bytesPerValue);
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
		Logger.log("Positions: " + Arrays.toString(positions));
		for (int i = 0; i < positions.length; i++) {
			long occurences = Utils.variableBytes2Long(dataBytes, positionLength + (i * bytesPerValue), bytesPerValue);
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
		positionLength = getPositionLength(positionEncoding);
		dataBytes = new byte[positionLength + (positionCount * bytesPerValue)];
		// Counts the positions != 0
		int positionIndex = 0;
		for (int i = 0; i < occurences.length; i++) {
			if (occurences[i] != 0) {
				if (positionEncoding == PositionEncoding.BITMAP) {
					setBitmapBit(i);
				} else if (positionEncoding == PositionEncoding.LIST) {
					dataBytes[positionIndex] = (byte) i;
				}
				Utils.writeLongIntoBytes(occurences[i], dataBytes, positionLength + (positionIndex * bytesPerValue),
						bytesPerValue);
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
		updateMetadataBits();
	}

	/**
	 * @return The value of the bytes-per-value column in the current {@link #row}. The offset of -1 is removed.
	 */
	private byte extractBytesPerValue() {
		return (byte) ((metadataBits & ((1 << VALUE_LENGTH_COLUMN_BITLENGTH) - 1)) + 1);
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
		metadataBits &= (1 << 31) >> (31 - VALUE_LENGTH_COLUMN_BITLENGTH);
		// Insert new value
		metadataBits |= bytesPerValue - 1;
		updateMetadataBits();
	}

	/**
	 * Writes the current {@link #metadataBits} into the {@link #row} array.
	 */
	private void updateMetadataBits() {
		Utils.writeLongIntoBytes(metadataBits, row, 0, metadataLength);
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
			// Stores the index of the current occurence value / counts positions != 0
			int valueIndex = 0;
			for (int i = 0; i < positionLength; i++) {
				int j = 0;
				// Skip the filled zeroes in the beginning
				int numberOfFillZeroes = (positionBitmapLength * Byte.SIZE) - positionBitmapBitLength;
				if (i == 0) {
					j = numberOfFillZeroes;
				}
				for (; j < Byte.SIZE; j++) {
					if ((dataBytes[i] & (0x80 >>> j)) != 0) {
						positions[valueIndex] = ((i * Byte.SIZE) + j) - numberOfFillZeroes;
						valueIndex++;
					}
				}
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
		dataBytes[bitmapEntryArrayIndex] |= 1 << (8 - bitmapEntryOffset - 1);
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
		return Utils.countOnesUntil(dataBytes, bitmapEntryArrayIndex, bitmapEntryOffset);
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
				return Utils.countOnesUntil(dataBytes, bitmapEntryArrayIndex, bitmapEntryOffset);
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

}
