package de.uni_koblenz.west.koral.master.statisticsDB.impl;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import de.uni_koblenz.west.koral.common.utils.ReusableIDGenerator;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatisticsDatabase;

public class MultiFileGraphStatisticsDatabase implements GraphStatisticsDatabase {

	/**
	 * How many bytes are used for the data in the main/index file. This space is used for either file offset or
	 * occurences values.
	 */
	private static final int ROW_DATA_LENGTH = 8;

	/**
	 * How many bits are used for the column that describes how many bytes are used per occurence value.
	 */
	private static final int VALUE_LENGTH_COLUMN_BITLENGTH = 3;

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

	private final String statisticsDirPath;

	private final File statisticsMainFile;

	private RandomAccessFile statisticsMain;

	private final LinkedList<File> statisticsExtraFiles;

	private final Map<Short, RandomAccessFile> statisticsExtra;

	private final Map<Short, ReusableIDGenerator> extraFilesFreeSpace;

	private final short numberOfChunks;

	static enum ResourceType {
		SUBJECT(0), PROPERTY(1), OBJECT(2);

		private int position;

		private ResourceType(int position) {
			this.position = position;
		}

		public int position() {
			return position;
		}
	}

	static enum PositionEncoding {
		BITMAP, LIST
	}

	public MultiFileGraphStatisticsDatabase(String statisticsDir, short numberOfChunks) {
		this.numberOfChunks = numberOfChunks;

		// -1 Offset for using zero as well
		int maxPositionCount = (3 * numberOfChunks) - 1;
		positionCountBitLength = 32 - Integer.numberOfLeadingZeros(maxPositionCount);
		metadataLength = (int) Math.ceil((positionCountBitLength + VALUE_LENGTH_COLUMN_BITLENGTH) / 8);
		positionBitmapLength = (int) Math.ceil((3 * numberOfChunks) / 8.0);
		mainfileRowLength = metadataLength + ROW_DATA_LENGTH;

		File statisticsDirFile = new File(statisticsDir);
		if (!statisticsDirFile.exists()) {
			statisticsDirFile.mkdirs();
		}
		try {
			statisticsDirPath = statisticsDirFile.getCanonicalPath() + File.separator;
			statisticsMainFile = new File(statisticsDirPath + "statistics");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		statisticsExtraFiles = new LinkedList<>();
		statisticsExtra = new TreeMap<>();
		extraFilesFreeSpace = new TreeMap<>();

		createStatistics();
	}

	private void createStatistics() {
		try {
			statisticsMain = new RandomAccessFile(statisticsMainFile, "rw");
		} catch (IOException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	@Override
	public void incrementSubjectCount(long subject, int chunk) {
		incrementOccurences(subject, ResourceType.SUBJECT, chunk);

	}

	@Override
	public void incrementPropertyCount(long property, int chunk) {
		incrementOccurences(property, ResourceType.PROPERTY, chunk);

	}

	@Override
	public void incrementObjectCount(long object, int chunk) {
		incrementOccurences(object, ResourceType.OBJECT, chunk);

	}

	private void incrementOccurences(long resource, ResourceType resourceType, int chunk) {
		try {
			// Try to read row
			byte[] row = readRow(statisticsMain, resource, mainfileRowLength);
			// If row doesn't exist yet, create it
			if (row == null) {
				// File should already point exactly at the ending where resource will be
				// inserted (because of readRow())
				assert (statisticsMain.getFilePointer() / mainfileRowLength) == resource;
				row = new byte[mainfileRowLength];
				// Position count and bytesPerValue are already zero (-> one)
				// Set first entry of position list
				row[metadataLength] = (byte) (getColumnNumber(resourceType, chunk));
				// Occurs once (until now)
				row[metadataLength + 1] = 1;
				statisticsMain.write(row);
				return;
			}

			// read occurence + increment
			int positionCount = extractPositionCount(row);
			byte bytesPerValue = extractBytesPerValue(row);

			PositionEncoding positionEncoding = optimalPositionEncoding(positionCount);
			int positionLength = 0;
			if (positionEncoding == PositionEncoding.BITMAP) {
				positionLength = positionBitmapLength;
			} else if (positionEncoding == PositionEncoding.LIST) {
				positionLength = positionCount;
			}

			int dataLength = (positionCount * bytesPerValue) + positionLength;
			// This array will contain the position info + the values
			byte[] dataBytes = readOccurenceData(row, dataLength);
			int currentOccurenceValueIndex = -1;
			int columnNumber = getColumnNumber(resourceType, chunk);
			if (positionEncoding == PositionEncoding.BITMAP) {
				// Find corresponding entry in bitmap
				int bitmapEntryArrayIndex = columnNumber / 8;
				byte entryOffset = (byte) (columnNumber % 8);
				byte bitmapEntry = (byte) (dataBytes[bitmapEntryArrayIndex] & (1 << (8 - entryOffset - 1)));
				// If that exists, find position of corresponding value
				if (bitmapEntry != 0) {
					currentOccurenceValueIndex = countOnesUntil(dataBytes, bitmapEntryArrayIndex, entryOffset);
				}
			} else if (positionEncoding == PositionEncoding.LIST) {
				for (int i = 0; i < positionLength; i++) {
					if (dataBytes[i] == columnNumber) {
						currentOccurenceValueIndex = i;
						break;
					}
				}
			}

			///////////////////////////////////////////////////////////////////////////////////////////////////////////
			// Create new row by updating the meta bytes of the old row array, and either creating a new array for the
			// data bytes or updating the old one.
			///////////////////////////////////////////////////////////////////////////////////////////////////////////
			byte[] newDataBytes;
			if (currentOccurenceValueIndex < 0) {
				// Resource never occured at this column position yet
				positionCount++;
				// Write new position count into meta column
				updatePositionCount(row, positionCount);
				if (optimalPositionEncoding(positionCount) != positionEncoding) {
					// Data bytes have to be completely rewritten
					// Extract all info of old data bytes
					long[] oldOccurences = decodeOccurenceData(dataBytes, positionEncoding, positionLength,
							bytesPerValue);
					// Encode into newDataBytes with new encoding
					newDataBytes = encodeOccurenceData(oldOccurences, optimalPositionEncoding(positionCount),
							positionCount, bytesPerValue);
				} else {
					// Copy old data bytes into newDatayBytes
					// Insert value into newDataBytes at the correct position
					if (positionEncoding == PositionEncoding.BITMAP) {
						// Set bitmap bit
						setBitmapBit(dataBytes, getColumnNumber(resourceType, chunk));
						currentOccurenceValueIndex = getValueIndexOfColumn(dataBytes, columnNumber);
						// Prepare new data bytes that will need space for one more value
						newDataBytes = new byte[dataLength + bytesPerValue];
						System.arraycopy(dataBytes, 0, newDataBytes, 0, dataLength);
						// Make space for new occurence value
						int currentOccurenceOffset = metadataLength + (currentOccurenceValueIndex * bytesPerValue);
						moveBytesRight(newDataBytes, currentOccurenceOffset, positionCount - currentOccurenceValueIndex,
								bytesPerValue);
						// Insert new occurence
						writeLongIntoBytes(1, newDataBytes, currentOccurenceOffset, bytesPerValue);
					} else if (positionEncoding == PositionEncoding.LIST) {
						// Prepare new data bytes that will have space for one more position byte and one more value
						newDataBytes = new byte[dataLength + 1 + bytesPerValue];
						System.arraycopy(dataBytes, 0, newDataBytes, 0, dataLength);
						// Make space for new position byte
						moveBytesRight(newDataBytes, positionLength, positionCount, 1);
						// Extend position list with new position
						newDataBytes[positionLength] = (byte) columnNumber;
						positionLength++;
						// Extend value list with new value
						newDataBytes[positionLength + positionCount] = 1;
					}
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
					long[] oldOccurences = decodeOccurenceData(dataBytes, positionEncoding, positionLength,
							bytesPerValue);
					// Update current occurence
					oldOccurences[currentOccurenceValueIndex] += 1;
					bytesPerValue++;
					// Rewrite values
					newDataBytes = new byte[positionLength + (positionCount * bytesPerValue)];
					// Copy old position info
					System.arraycopy(dataBytes, 0, newDataBytes, 0, positionLength);
					for (int i = 0; i < positionCount; i++) {
						writeLongIntoBytes(oldOccurences[i], newDataBytes, i * bytesPerValue, bytesPerValue);
					}
					// Increase bytesPerValue column
					updateBytesPerValue(row, bytesPerValue);
				} else {
					// Update data bytes/the one occurence value
					writeLongIntoBytes(occurences, dataBytes, currentOccurenceValueIndex * bytesPerValue,
							bytesPerValue);
				}

			}

			byte[] newRow = new byte[metadataLength + positionLength + (positionCount * bytesPerValue)];
			// TODO: check if data is too long for index/extra file
			// TODO: write new data (evtl. RLE)
			// TODO: potentially remove/change old row
		} catch (IOException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	@Override
	public void incrementNumberOfTriplesPerChunk(int chunk) {
		// TODO Auto-generated method stub

	}

	@Override
	public long[] getChunkSizes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long[] getStatisticsForResource(long id) {
		try {
			byte[] row = readRow(statisticsMain, id, mainfileRowLength);
			if (row == null) {
				return new long[3 * numberOfChunks];
			}
			int positionCount = extractPositionCount(row);
			byte bytesPerValue = extractBytesPerValue(row);

			PositionEncoding positionEncoding = optimalPositionEncoding(positionCount);
			int positionLength = 0;
			if (positionEncoding == PositionEncoding.BITMAP) {
				positionLength = positionBitmapLength;
			} else if (positionEncoding == PositionEncoding.LIST) {
				positionLength = positionCount;
			}

			int dataLength = (positionCount * bytesPerValue) + positionLength;
			// This array will contain the position info + the values
			byte[] dataBytes = readOccurenceData(row, dataLength);
			return decodeOccurenceData(dataBytes, positionEncoding, positionLength, bytesPerValue);
		} catch (IOException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	private byte[] readRow(RandomAccessFile file, long id, int rowLength) throws IOException {
		long mainFileOffset = id * mainfileRowLength;
		statisticsMain.seek(mainFileOffset);
		byte[] row = new byte[mainfileRowLength];
		try {
			statisticsMain.readFully(row);
		} catch (EOFException e) {
			// Resource does not have an entry (yet)
			return null;
		}
		return row;
	}

	private int getColumnNumber(ResourceType resourceType, int chunk) {
		return (resourceType.position() * numberOfChunks) + chunk;
	}

	private int extractPositionCount(byte[] row) {
		long metadataBits = variableBytes2Long(row, 0, metadataLength);
		return (int) metadataBits >>> ((metadataLength * 8) - positionCountBitLength);
	}

	/**
	 * Replaces the old positionCount value in <code>row</code> with the given new one.
	 *
	 * @param row
	 *            The read row that will be updated
	 * @param newPositionCount
	 *            The new value for the position count column
	 */
	private void updatePositionCount(byte[] row, long newPositionCount) {
		long metadataBits = variableBytes2Long(row, 0, metadataLength);
		int postionCountOffset = (metadataLength * 8) - positionCountBitLength;
		// This bitfilter will be zero everywhere, except for the bytesPerValue bits, to remove the previous position
		// count bits.
		long bitFilter = (1L << postionCountOffset) - 1;
		// Remove old position count
		metadataBits &= bitFilter;
		// Insert new position count
		metadataBits |= newPositionCount << postionCountOffset;
		writeLongIntoBytes(metadataBits, row, 0, metadataLength);
	}

	private byte extractBytesPerValue(byte[] row) {
		// This works only if the bits are all in the same (last) byte
		assert VALUE_LENGTH_COLUMN_BITLENGTH <= 8;
		return (byte) (row[metadataLength - 1] & ((1 << VALUE_LENGTH_COLUMN_BITLENGTH) - 1));
	}

	/**
	 * Replaces the old bytesPerValue value in <code>row</code> with the given new one.
	 *
	 * @param row
	 *            The read row that will be updated
	 * @param newBytesPerValue
	 *            The new value for the bytesPerValue column
	 */
	private void updateBytesPerValue(byte[] row, byte newBytesPerValue) {
		// This works only if the bits are all in the same (last) byte
		assert VALUE_LENGTH_COLUMN_BITLENGTH <= 8;
		// Remove old value
		row[metadataLength - 1] &= (1 << 31) >> (31 - VALUE_LENGTH_COLUMN_BITLENGTH);
		// Insert new value
		row[metadataLength] |= newBytesPerValue;
	}

	/**
	 * Detects the optimal position encoding by comparing each option's needed bytes.
	 *
	 * @param positionCount
	 *            How many positions have occurence values >0
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
	 * Extracts all positions/column numbers which have an occurence value >1, i.e. have an associated value in the data
	 * bytes.
	 *
	 * @param dataBytes
	 *            The data bytes, with positions encoded in either bitmap or list
	 * @param positionEncoding
	 *            How the positions are encoded
	 * @param positionLength
	 *            How long the position info is in bytes
	 * @return An array of all columns with occurences, sorted by the order of their corresponding values (second
	 *         element of this maps to second occurence value). The length is positionCount. Example: [1,1,2,1,5,3,1]
	 */
	private int[] extractPositions(byte[] dataBytes, PositionEncoding positionEncoding, int positionLength) {
		int[] positions = new int[positionLength];
		if (positionEncoding == PositionEncoding.BITMAP) {
			for (int i = 0; i < positionLength; i++) {
				positions[i] = dataBytes[i];
			}
		} else if (positionEncoding == PositionEncoding.BITMAP) {
			long positionBitmap = variableBytes2Long(dataBytes, 0, positionLength);
			// Stores the position in the bitmap i.e. the index where the value will be
			// stored
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
	 * @param dataBytes
	 *            The data bytes that contain the position bitmap
	 * @param columnNumber
	 *            Which column will be read
	 * @return The bit as byte
	 */
	private byte getBitmapBit(byte[] dataBytes, int columnNumber) {
		int bitmapEntryArrayIndex = columnNumber / 8;
		byte entryOffset = (byte) (columnNumber % 8);
		return (byte) (dataBytes[bitmapEntryArrayIndex] & (1 << (8 - entryOffset - 1)));
	}

	/**
	 * Sets the bit at columnNumber to 1.
	 *
	 * @param dataBytes
	 *            The data bytes containing the position bitmap to change
	 * @param columnNumber
	 *            Which column to update
	 */
	private void setBitmapBit(byte[] dataBytes, int columnNumber) {
		// dataBytes might be too long for conversion to long, so we have to change the array directly
		int bitmapEntryArrayIndex = columnNumber / 8;
		byte entryOffset = (byte) (columnNumber % 8);
		dataBytes[bitmapEntryArrayIndex] |= 1 << (8 - entryOffset - 1);
	}

	/**
	 * Returns the index of the value that corresponds to the specified columnNumber, based on a bitmap given in
	 * dataBytes.
	 *
	 * @param dataBytes
	 *            Data bytes containing a position bitmap
	 * @param columnNumber
	 *            The column of the wanted occurence value
	 * @return
	 */
	private int getValueIndexOfColumn(byte[] dataBytes, int columnNumber) {
		int bitmapEntryArrayIndex = columnNumber / 8;
		byte entryOffset = (byte) (columnNumber % 8);
		return countOnesUntil(dataBytes, bitmapEntryArrayIndex, entryOffset);
	}

	/**
	 * Retrieves the occurence data bytes for an already read row. This method recognizes if that data is stored in the
	 * row, or in an extra file and retrieves it either way.
	 *
	 * @param row
	 *            A read row of the index/main file
	 * @param dataLength
	 *            How long the data is in bytes, can be calculated with the meta info of the row
	 * @return The extracted bytes that represent the position info and occurence values. There may be unused bytes at
	 *         the end
	 * @throws IOException
	 */
	private byte[] readOccurenceData(byte[] row, int dataLength) throws IOException {
		byte[] dataBytes = new byte[dataLength]; // Note: This array may be too long
		if (dataLength <= ROW_DATA_LENGTH) {
			// Values are here
			System.arraycopy(row, metadataLength, dataBytes, 0, dataLength);
		} else {
			// Values are in extra file
			// Extract data bytes as offset
			long extraOffset = variableBytes2Long(row, metadataLength, ROW_DATA_LENGTH);
			// TODO: fix row[0]
			RandomAccessFile extra = statisticsExtra.get((short) row[0]);
			if (extra != null) {
				// Check if random access file is open
				if (!extra.getFD().valid()) {
					// Open extra file and store it
					File extraFile = new File(statisticsDirPath + row[0]);
					extra = new RandomAccessFile(extraFile, "rw");
					statisticsExtra.put((short) row[0], extra);
				}
				extra.seek(extraOffset);
				extra.readFully(dataBytes);
			}
		}
		return dataBytes;
	}

	/**
	 * Decodes the data bytes of a row and returns the positioned occurence values.
	 *
	 * @param dataBytes
	 *            The data bytes of a row
	 * @param positionEncoding
	 *            How the position is encoded
	 * @param positionLength
	 *            How many bytes the position info occupies
	 * @param bytesPerValue
	 *            How many bytes are used per value
	 * @return An array with the length of all columns, with occurence values at their corresponding columns position,
	 *         e.g. [0, 1, 1, 0, 0, 0, 7, 0, 2]
	 */
	private long[] decodeOccurenceData(byte[] dataBytes, PositionEncoding positionEncoding, int positionLength,
			byte bytesPerValue) {
		long[] occurenceValues = new long[3 * numberOfChunks];
		int[] positions = extractPositions(dataBytes, positionEncoding, positionLength);
		for (int i = 0; i < positions.length; i++) {
			long occurences = variableBytes2Long(dataBytes, positionLength + i, bytesPerValue);
			occurenceValues[positions[i]] = occurences;
		}
		return occurenceValues;
	}

	/**
	 * Encodes occurence data into the data byte format, either with a bitmap or list encoding (as specified).
	 *
	 * @param occurences
	 *            An array that maps each column number to its occurences, like the output of
	 *            {@link #decodeOccurenceData(byte[], PositionEncoding, int, byte)}.
	 * @param positionEncoding
	 *            How the data should be encoded
	 * @param positionCount
	 *            How many positions/columns are != 0
	 * @param bytesPerValue
	 *            How many bytes a value needs
	 * @return A data byte array with the length only as long as needed
	 */
	private byte[] encodeOccurenceData(long[] occurences, PositionEncoding positionEncoding, int positionCount,
			byte bytesPerValue) {
		int positionLength = 0;
		if (positionEncoding == PositionEncoding.BITMAP) {
			positionLength = positionBitmapLength;
		} else if (positionEncoding == PositionEncoding.LIST) {
			positionLength = positionCount;
		}
		byte[] newDataBytes = new byte[positionLength + (positionCount * bytesPerValue)];
		if (positionEncoding == PositionEncoding.BITMAP) {
			for (int i = 0; i < occurences.length; i++) {
				if (occurences[i] != 0) {
					setBitmapBit(newDataBytes, i);
					writeLongIntoBytes(occurences[i], newDataBytes, positionLength + (i * bytesPerValue),
							bytesPerValue);
				}
			}
		} else if (positionEncoding == PositionEncoding.LIST) {
			for (int i = 0; i < occurences.length; i++) {
				if (occurences[i] != 0) {
					newDataBytes[i] = (byte) i;
					writeLongIntoBytes(occurences[i], newDataBytes, positionLength + (i * bytesPerValue),
							bytesPerValue);
				}
			}
		}
		return newDataBytes;
	}

	@Override
	public void clear() {
		close();
		statisticsMainFile.delete();
		for (File f : statisticsExtraFiles) {
			f.delete();
		}
		statisticsExtraFiles.clear();
		statisticsExtra.clear();
		extraFilesFreeSpace.clear();
		createStatistics();

	}

	@Override
	public void close() {
		try {
			statisticsMain.close();
			for (RandomAccessFile f : statisticsExtra.values()) {
//				if (f != null) {
				f.close();
//				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	private static int countOnes(byte[] array) {
		return countOnesUntil(array, array.length - 1, (byte) Byte.SIZE);
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
	 *            How many indexes each byte will move
	 */
	private static void moveBytesRight(byte[] bytes, int startIndex, int length, int offset) {
		// Iterate interval in reverse order
		for (int i = startIndex + length; i >= startIndex; i--) {
			bytes[i + offset] = bytes[i];
		}
	}

}
