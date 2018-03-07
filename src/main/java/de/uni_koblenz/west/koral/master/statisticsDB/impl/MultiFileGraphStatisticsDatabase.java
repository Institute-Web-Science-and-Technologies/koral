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
	 * How many bytes are used for metadata per row.
	 */
	private static final int ROW_METADATA_LENGTH = 1;

	/**
	 * How many bytes are used for the data in the main/index file. This space is
	 * used for either file offset or occurences values.
	 */
	private static final int ROW_DATA_LENGTH = 8;

	/**
	 * Length of each row in the main/index file.
	 */
	private static final int MAINFILE_ROW_LENGTH = ROW_METADATA_LENGTH + ROW_DATA_LENGTH;

	private final File statisticsMainFile;

	private RandomAccessFile statisticsMain;

	private final LinkedList<File> statisticsExtraFiles;

	private final Map<Short, RandomAccessFile> statisticsExtra;

	private final Map<Short, ReusableIDGenerator> extraFilesFreeSpace;

	private final short numberOfChunks;

	private final int positionBitmapLength;

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
		File statisticsDirFile = new File(statisticsDir);
		if (!statisticsDirFile.exists()) {
			statisticsDirFile.mkdirs();
		}
		this.numberOfChunks = numberOfChunks;
		positionBitmapLength = (int) Math.ceil((3 * numberOfChunks) / 8.0);

		try {
			statisticsMainFile = new File(
					statisticsDirFile.getCanonicalPath() + File.separator + "statistics");
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
			long mainFileOffset = id * MAINFILE_ROW_LENGTH;
			statisticsMain.seek(mainFileOffset);
			byte[] row = new byte[MAINFILE_ROW_LENGTH];
			try {
				statisticsMain.readFully(row);
			} catch (EOFException e) {
				// Resource does not have an entry (yet)
				return new long[3 * numberOfChunks];
			}
			byte numberPositions = (byte) (row[0] & 0xF8); // Extract first 5 bits
			byte bytesPerValue = (byte) (row[0] & 0x07); // Extract the other 3 bits

			PositionEncoding positionEncoding;
			int positionLength;
			if (numberPositions <= positionBitmapLength) {
				// List is shorter
				positionLength = numberPositions;
				positionEncoding = PositionEncoding.LIST;
			} else {
				// Bitmap is shorter
				positionLength = positionBitmapLength;
				positionEncoding = PositionEncoding.BITMAP;
			}

			// This array will contain the position info + the values
			byte[] dataBytes = new byte[ROW_DATA_LENGTH]; // Note: This array may be too long
			int informationLength = (numberPositions * bytesPerValue) + positionLength;
			if (informationLength <= ROW_DATA_LENGTH) {
				// Values are here
				System.arraycopy(row, ROW_METADATA_LENGTH, dataBytes, 0, dataBytes.length);
			} else {
				// Values are in extra file
				// Extract data bytes as offset
				long extraOffset = NumberConversion.bytes2long(row, ROW_METADATA_LENGTH);
				// TODO: extra file may be null/needs to be opened
				RandomAccessFile extra = statisticsExtra.get(row[0]);
				extra.seek(extraOffset);
				extra.readFully(dataBytes);
			}

			long[] occurenceValues = new long[3 * numberOfChunks];
			// Extract position info
			if (positionEncoding == PositionEncoding.LIST) {
				for (int i = 0; i < positionLength; i++) {
					byte position = dataBytes[i];
					// The position infos at i map 1:1 to the following values (after
					// positionLength)
					long occurences = variableBytes2Long(dataBytes, positionLength + i,
							bytesPerValue);
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
				for (int filterIndex = 1 << (positionBitmapLength * 8); // Start value is 1000...0
						filterIndex > 1; // Continue until filter index is 0
						filterIndex >>>= 1 // Move the 1 one to the right each step
						) {
					if ((filterIndex & positionBitmap) > 0) {
						long occurences = variableBytes2Long(dataBytes, positionLength + valueIndex,
								bytesPerValue);
						occurenceValues[position] = occurences;
						valueIndex++;
					}
					position++;
				}
			}
			return occurenceValues;
		} catch (IOException e) {
			close();
			throw new RuntimeException(e);
		}
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

	// TODO
	private long variableBytes2Long(byte[] bytes, int startIndex, int length) {
		return 0L;
	}

	// TODO
	private long[] byteArray2LongArray(byte[] array) {
		long[] result = new long[array.length];
		for (int i = 0; i < array.length; i++) {
			result[i] = NumberConversion.bytes2long(array);
		}
		return result;
	}

}
