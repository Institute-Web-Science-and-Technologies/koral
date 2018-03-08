package de.uni_koblenz.west.koral.master.statisticsDB.impl;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;
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
	private final int rowMetadataLength;

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
		rowMetadataLength = (int) Math.ceil((positionCountBitLength + VALUE_LENGTH_COLUMN_BITLENGTH) / 8);
		positionBitmapLength = (int) Math.ceil((3 * numberOfChunks) / 8.0);
		mainfileRowLength = rowMetadataLength + ROW_DATA_LENGTH;

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
				// File should already point exactly at the ending, where resource will be
				// inserted
				assert (statisticsMain.getFilePointer() / mainfileRowLength) == resource;
				row = new byte[mainfileRowLength];
				// Position count and bytesPerValue are already zero
				// Set first entry of position list
				row[rowMetadataLength] = (byte) ((resourceType.position() * numberOfChunks) + chunk);
				// Occurs once (until now)
				row[rowMetadataLength + 1] = 1;
				statisticsMain.write(row);
				return;
			}
			// TODO: read occurence + increment
			int positionCount = extractPositionCount(row);
			byte bytesPerValue = extractBytesPerValue(row);
			// TODO: check if position encoding needs to change
			// TODO: check if data is too long for index/extra file
			// TODO: write new data
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

	private int extractPositionCount(byte[] row) {
		long metadataBits = variableBytes2Long(row, 0, rowMetadataLength);
		return (int) metadataBits >>> ((rowMetadataLength * 8) - positionCountBitLength);
	}

	private byte extractBytesPerValue(byte[] row) {
		// This works only if the bits are all in the same (last) byte
		assert VALUE_LENGTH_COLUMN_BITLENGTH <= 8;
		return (byte) (row[row.length - 1] & 0b111);
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
	 * Retrieves the occurence data bytes for an already read row. This method recognizes if that data is stored in the
	 * row, or in an extra file and retrieves it either way.
	 *
	 * @param row
	 *            A read row of the index/main file
	 * @param dataLength
	 *            How long the data is in bytes, can be calculated with the meta info of the row
	 * @return The extracted bytes that represent the position info and occurence values
	 * @throws IOException
	 */
	private byte[] readOccurenceData(byte[] row, int dataLength) throws IOException {
		byte[] dataBytes = new byte[ROW_DATA_LENGTH]; // Note: This array may be too long
		if (dataLength <= ROW_DATA_LENGTH) {
			// Values are here
			System.arraycopy(row, rowMetadataLength, dataBytes, 0, dataBytes.length);
		} else {
			// Values are in extra file
			// Extract data bytes as offset
			long extraOffset = NumberConversion.bytes2long(row, rowMetadataLength);
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

	private long[] decodeOccurenceData(byte[] dataBytes, PositionEncoding positionEncoding, int positionLength,
			byte bytesPerValue) {
		long[] occurenceValues = new long[3 * numberOfChunks];
		// Extract position info
		if (positionEncoding == PositionEncoding.LIST) {
			for (int i = 0; i < positionLength; i++) {
				byte position = dataBytes[i];
				// The position infos at i map 1:1 to the following values (after
				// positionLength)
				long occurences = variableBytes2Long(dataBytes, positionLength + i, bytesPerValue);
				occurenceValues[position] = occurences;
			}
		} else if (positionEncoding == PositionEncoding.BITMAP) {
			long positionBitmap = variableBytes2Long(dataBytes, 0, positionLength);
			// Stores the position in the bitmap i.e. the index where the value will be
			// stored
			int position = 0;
			// Stores the index of the current occurence value
			int valueIndex = 0;
			// Iterate over each bit in the position bitmap with a filter index
			for (long filterIndex = 1 << (positionBitmapLength * 8); // Start value is 1000...0
					filterIndex > 1; // Continue until filter index is at the last bit
					filterIndex >>>= 1 // Move the 1 one to the right each step
					) {
				if ((filterIndex & positionBitmap) > 0) {
					long occurences = variableBytes2Long(dataBytes, positionLength + valueIndex, bytesPerValue);
					occurenceValues[position] = occurences;
					valueIndex++;
				}
				position++;
			}
		}
		return occurenceValues;
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
			// Shift byte based on its position. -1 because we dont need to shift the last
			// byte
			result += bytes[startIndex + i] << (Byte.BYTES * (length - 1 - i));
		}
		return result;
	}

}
