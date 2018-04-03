package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatisticsDatabase;

/**
 * The main class of this implementation, manages the {@link StatisticsRowManager} and persistence through
 * {@link FileManager}.
 *
 * @author philipp
 *
 */
public class MultiFileGraphStatisticsDatabase implements GraphStatisticsDatabase {

	private static final int DEFAULT_ROW_DATA_LENGTH = 8;

	private final String statisticsDirPath;

	private final short numberOfChunks;

	private final long[] triplesPerChunk;

	private final Path triplesPerChunkFile;

	private final FileManager fileManager;

	private final StatisticsRowManager rowManager;

	private final int mainfileRowLength;

	private final long maximumAddressableExtraFileRows;

	static enum ResourceType {
		SUBJECT(0), PROPERTY(1), OBJECT(2);

		private final int position;

		private ResourceType(int position) {
			this.position = position;
		}

		public int position() {
			return position;
		}
	}

	public MultiFileGraphStatisticsDatabase(String statisticsDir, short numberOfChunks, int rowDataLength) {
		this.numberOfChunks = numberOfChunks;

		maximumAddressableExtraFileRows = (1 << (rowDataLength * Byte.SIZE)) - 1;

		rowManager = new StatisticsRowManager(numberOfChunks, rowDataLength);
		mainfileRowLength = rowManager.getMainFileRowLength();

		File statisticsDirFile = new File(statisticsDir);
		if (!statisticsDirFile.exists()) {
			statisticsDirFile.mkdirs();
		}
		try {
			statisticsDirPath = statisticsDirFile.getCanonicalPath() + File.separator;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		fileManager = new FileManager(statisticsDirPath);

		triplesPerChunkFile = Paths.get(statisticsDirPath + "triplesPerChunk");
		triplesPerChunk = loadTriplesPerChunk();
	}

	public MultiFileGraphStatisticsDatabase(String statisticsDir, short numberOfChunks) {
		this(statisticsDir, numberOfChunks, DEFAULT_ROW_DATA_LENGTH);
	}

	private long[] loadTriplesPerChunk() {
		long[] triplesPerChunk = new long[numberOfChunks];
		if (!Files.exists(triplesPerChunkFile)) {
			return triplesPerChunk;
		}
		System.out.println("Found triplesPerChunk file, reading it...");
		try {
			byte[] content = Files.readAllBytes(triplesPerChunkFile);
			for (int i = 0; i < (content.length / Long.BYTES); i++) {
				triplesPerChunk[i] = NumberConversion.bytes2long(content, i * Long.BYTES);
			}
			return triplesPerChunk;
		} catch (IOException e) {
			System.err.println("Error reading triples-per-chunk file:");
			e.printStackTrace();
			return new long[numberOfChunks];
		}
	}

	@Override
	public void incrementNumberOfTriplesPerChunk(int chunk) {
		triplesPerChunk[chunk]++;
	}

	@Override
	public long[] getChunkSizes() {
		return triplesPerChunk;
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

	private void incrementOccurences(long resourceId, ResourceType resourceType, int chunk) {
		try {
			boolean rowFound = loadRow(resourceId);
			if (!rowFound) {
				byte[] row = rowManager.create(resourceType, chunk);
				fileManager.writeIndexRow(resourceId, row);
				return;
			}
			long fileIdRead = rowManager.getFileId();
			long extraFileRowId = rowManager.getExternalFileRowId();
			rowManager.incrementOccurence(resourceType, chunk);
			if (!rowManager.isDataExternal()) {
				if (rowManager.isTooLongForMain()) {
					long newExtraFileRowId = fileManager.writeExternalRow(rowManager.getFileId(),
							rowManager.getDataBytes());
					checkIfDataBytesLengthIsEnough(newExtraFileRowId);
//					Logger.log("I->E: " + Arrays.toString(rowManager.getDataBytes()));
					rowManager.updateRowExtraOffset(newExtraFileRowId);
				} else {
					rowManager.mergeDataBytesIntoRow();
				}
			} else {
				long fileIdWrite = rowManager.getFileId();
				long newExtraFileRowID = extraFileRowId;
				if (fileIdWrite != fileIdRead) {
					// Move entry into different extra file
					fileManager.deleteExternalRow(fileIdRead, extraFileRowId);
					newExtraFileRowID = fileManager.writeExternalRow(fileIdWrite, rowManager.getDataBytes());
					checkIfDataBytesLengthIsEnough(newExtraFileRowID);
				} else {
					// Overwrite old extra file entry
					fileManager.writeExternalRow(fileIdWrite, newExtraFileRowID, rowManager.getDataBytes());
				}
//				Logger.log("->E " + fileIdWrite + "/" + newExtraFileRowID + ": "
//						+ Arrays.toString(rowManager.getDataBytes()));
				// Write new offset into index row
				rowManager.updateRowExtraOffset(newExtraFileRowID);
			}
//			Logger.log("New Row: " + Arrays.toString(rowManager.getRow()));
			fileManager.writeIndexRow(resourceId, rowManager.getRow());

		} catch (IOException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	@Override
	public long[] getStatisticsForResource(long id) {
		boolean rowFound = loadRow(id);
		if (!rowFound) {
			return new long[3 * numberOfChunks];
		}
		return rowManager.decodeOccurenceData();
	}

	/**
	 * Loads the specified row from the file, and loads it into the rowManager if it was found. If the rowManager
	 * detects that the row refers to an extra file, the row from the extra file is loaded as well and given to the
	 * rowManager. Note that a row filled with only zeroes counts as not existing.
	 *
	 * @param id
	 * @return True if the row was found.
	 */
	private boolean loadRow(long id) {
//		Logger.log("----- ID " + id);
		try {
			byte[] row = fileManager.readIndexRow(id, mainfileRowLength);
			if ((row == null) || Utils.isArrayZero(row)) {
				return false;
			}
			boolean dataExternal = rowManager.load(row);
//			Logger.log("row " + id + ": " + Arrays.toString(rowManager.getRow()));
			if (dataExternal) {
				byte[] dataBytes = fileManager.readExternalRow(rowManager.getFileId(),
						rowManager.getExternalFileRowId(), rowManager.getDataLength());
				rowManager.loadExternalRow(dataBytes);
//				Logger.log("E Row: " + Arrays.toString(dataBytes));
			}
		} catch (IOException e) {
			close();
			throw new RuntimeException(e);
		}
		return true;
	}

	private void flush() {
		fileManager.flush();
		byte[] bytes = new byte[Long.BYTES * triplesPerChunk.length];
		for (int i = 0; i < triplesPerChunk.length; i++) {
			NumberConversion.long2bytes(triplesPerChunk[i], bytes, i * Long.BYTES);
		}
		try {
			Files.write(triplesPerChunkFile, bytes);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void clear() {
		close();
		try {
			FileUtils.cleanDirectory(new File(statisticsDirPath));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		fileManager.setup();
	}

	private void checkIfDataBytesLengthIsEnough(long newRowId) {
		if (newRowId > maximumAddressableExtraFileRows) {
			throw new IllegalArgumentException(
					"Too many extra file rows for the current data bytes length. Please set rowDataLength parameter higher.");
		}
	}

	public long getMaxId() {
		return (fileManager.getIndexFileLength()) / mainfileRowLength;
	}

	public long getIndexFileLength() {
		return fileManager.getIndexFileLength();
	}

	public Map<Long, Long> getFreeSpaceIndexLenghts() {
		return fileManager.getFreeSpaceIndexLengths();
	}

	@Override
	public void close() {
		try {
			flush();
		} finally {
			fileManager.close();
		}
	}

	/**
	 * Collects and returns a formatted statistical report on all the written entries.
	 *
	 * @return
	 */
	public String getStatistics() {
		long maxId = getMaxId();
		for (long id = 1; id <= maxId; id++) {
			boolean rowFound = loadRow(id);
			if (!rowFound) {
				continue;
			}
			rowManager.collectStatistics();
		}
		return rowManager.getStatistics();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("TriplesPerChunk ");
		for (long l : getChunkSizes()) {
			sb.append("\t").append(l);
		}
		sb.append("\n");
		sb.append("ResourceID");
		for (int i = 0; i < numberOfChunks; i++) {
			sb.append(";").append("subjectInChunk").append(i);
		}
		for (int i = 0; i < numberOfChunks; i++) {
			sb.append(";").append("propertyInChunk").append(i);
		}
		for (int i = 0; i < numberOfChunks; i++) {
			sb.append(";").append("objectInChunk").append(i);
		}
		sb.append(";").append("overallOccurrance");
		long maxId = getMaxId();
		for (long id = 1; id <= maxId; id++) {
			sb.append("\n");
			sb.append(id);
			for (long value : getStatisticsForResource(id)) {
				sb.append(";").append(value);
			}
		}
		return sb.toString();
	}

}
