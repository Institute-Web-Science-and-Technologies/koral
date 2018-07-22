package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

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

	private final Logger logger;

	private final String statisticsDirPath;

	private int numberOfChunks;

	private long[] triplesPerChunk;

	private final Path statisticsMetadataFile;

	private final FileManager fileManager;

	private final StatisticsRowManager rowManager;

	private final int mainfileRowLength;

	private int rowDataLength;

	private boolean dirty;

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

	public MultiFileGraphStatisticsDatabase(String statisticsDir, int numberOfChunks, int rowDataLength,
			long indexCacheSize, long extraFilesCacheSize, Logger logger) {
		this.logger = logger;

		// First, check if we have metadata for an existing database
		File statisticsDirFile = new File(statisticsDir);
		if (!statisticsDirFile.exists()) {
			statisticsDirFile.mkdirs();
		}
		try {
			statisticsDirPath = statisticsDirFile.getCanonicalPath() + File.separator;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		statisticsMetadataFile = Paths.get(statisticsDirPath + "statisticsMetadata");
		if (Files.exists(statisticsMetadataFile)) {
			loadStatisticsMetadata();
		} else {
			System.out.println("StatisticsMetadata file was not found.");
			if (logger != null) {
				logger.finest("StatisticsMetadata file was not found.");
			}
			this.numberOfChunks = numberOfChunks;
			this.rowDataLength = rowDataLength;
			triplesPerChunk = new long[numberOfChunks];
		}

		rowManager = new StatisticsRowManager(this.numberOfChunks, this.rowDataLength);
		mainfileRowLength = rowManager.getMainFileRowLength();

		// Calculate the theoretically maximal amount of extra files. Used for caching space distribution in the file
		// manager
		int maxValueBytesPerOccurenceValue = 1 << StatisticsRowManager.VALUE_LENGTH_COLUMN_BITLENGTH;
		int maxExtraFilesAmount = 3 * this.numberOfChunks * maxValueBytesPerOccurenceValue;
		fileManager = new FileManager(statisticsDirPath, mainfileRowLength, maxExtraFilesAmount,
				FileManager.DEFAULT_MAX_OPEN_FILES, indexCacheSize, extraFilesCacheSize, logger);

		dirty = false;
	}

	public MultiFileGraphStatisticsDatabase(String statisticsDir, short numberOfChunks, Logger logger) {
		this(statisticsDir, numberOfChunks, DEFAULT_ROW_DATA_LENGTH, FileManager.DEFAULT_INDEX_FILE_CACHE_SIZE,
				FileManager.DEFAULT_EXTRAFILES_CACHE_SIZE, logger);
	}

	private void loadStatisticsMetadata() {
		System.out.println("Found existing statisticsMetadata file, reading it...");
		if (logger != null) {
			logger.finest("Found existing statisticsMetadata file, reading it...");
		}
		byte[] content;
		try {
			content = Files.readAllBytes(statisticsMetadataFile);
		} catch (IOException e) {
			throw new RuntimeException("Error reading existing statisticsMetadata file: " + e);
		}
		rowDataLength = NumberConversion.bytes2int(content);
		numberOfChunks = (content.length - Integer.BYTES) / Long.BYTES;
		triplesPerChunk = new long[numberOfChunks];
		for (int i = 0; i < (content.length / Long.BYTES); i++) {
			triplesPerChunk[i] = NumberConversion.bytes2long(content, Integer.BYTES + (i * Long.BYTES));
		}
		System.out.println("Read triplesPerChunk: " + Arrays.toString(triplesPerChunk));
		System.out.println("Loaded existing statisticsMetadata. Found rowDataLength = " + rowDataLength
				+ ", numberOfChunks = " + numberOfChunks);
		if (logger != null) {
			logger.finest(
					"Loaded existing statisticsMetadata, numberOfChunks and rowDataLength parameters will be ignored. Found rowDataLength = "
							+ rowDataLength + ", numberOfChunks = " + numberOfChunks);
		}
	}

	@Override
	public void incrementNumberOfTriplesPerChunk(int chunk) {
		triplesPerChunk[chunk]++;
	}

	/**
	 * Sets the number of triples per chunk array, containing a long value for each existing chunk. The array is cloned
	 * for further internal use.
	 *
	 * @param triplesPerChunk
	 */
	public void setNumberOfTriplesPerChunk(long[] triplesPerChunk) {
		this.triplesPerChunk = triplesPerChunk.clone();
	}

	@Override
	public long[] getChunkSizes() {
		return triplesPerChunk;
	}

	@Override
	public long[] getStatisticsForResource(long id) {
		boolean rowFound;
		try {
			rowFound = loadRow(id);
		} catch (IOException e) {
			close();
			throw new RuntimeException(e);
		}
		if (!rowFound) {
			return new long[3 * numberOfChunks];
		}
		return rowManager.decodeOccurenceData();
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
		dirty = true;
		try {
			boolean rowFound = loadRow(resourceId);
			if (!rowFound) {
				rowManager.create(resourceType, chunk);
				if (rowManager.isTooLongForMain()) {
					insertEntryInExtraFile();
				}
				fileManager.writeIndexRow(resourceId, rowManager.getRow());
				return;
			}
			// Extract file id before incrementing for later comparison
			long fileIdRead = rowManager.getFileId();
			rowManager.incrementOccurence(resourceType, chunk);
			if (!rowManager.isDataExternal()) {
				if (rowManager.isTooLongForMain()) {
					insertEntryInExtraFile();
				} else {
					rowManager.mergeDataBytesIntoRow();
				}
			} else {
				long fileIdWrite = rowManager.getFileId();
				long extraFileRowId = rowManager.getExternalFileRowId();
				long newExtraFileRowID = extraFileRowId;
				if (fileIdWrite != fileIdRead) {
					// Move entry into different extra file
					fileManager.deleteExternalRow(fileIdRead, extraFileRowId);
					newExtraFileRowID = fileManager.writeExternalRow(fileIdWrite, rowManager.getDataBytes());
					checkIfDataBytesLengthIsEnough(newExtraFileRowID);
				} else {
					// Overwrite old extra file entry
					fileManager.writeExternalRow(fileIdWrite, extraFileRowId, rowManager.getDataBytes());
				}
//				SimpleLogger.log("->E " + fileIdWrite + "/" + newExtraFileRowID + ": "
//						+ Arrays.toString(rowManager.getDataBytes()));
				// Write new offset into index row
				rowManager.updateExtraRowId(newExtraFileRowID);
			}
//			SimpleLogger.log("New Row: " + Arrays.toString(rowManager.getRow()));
			fileManager.writeIndexRow(resourceId, rowManager.getRow());

		} catch (IOException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	/**
	 * Stores already computed statistics for a resource. Can be used for inserting statistics of an other statistic
	 * database implementation. Calling this multiple times for the same resource might result in orphaned storage space
	 * (perviously used extra file rows are not marked as deleted).
	 *
	 * @param resourceId
	 *            The resource which entry will be created
	 * @param occurences
	 *            Occurence values for each column, like the returned array of {@link #getStatisticsForResource(long)}.
	 */
	public void insertEntry(long resourceId, long[] occurences) {
		dirty = true;
		for (int c = 0; c < numberOfChunks; c++) {
			// S,P,O
			for (int i = 0; i < 3; i++) {
				triplesPerChunk[c] += occurences[(i * numberOfChunks) + c];
			}
		}
		try {
			rowManager.loadFromOccurenceData(occurences);
			if (rowManager.isTooLongForMain()) {
				insertEntryInExtraFile();
			} else {
				rowManager.mergeDataBytesIntoRow();
			}
			fileManager.writeIndexRow(resourceId, rowManager.getRow());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Loads the row of the specified resource from the file, and loads it into the rowManager if it was found. If the
	 * rowManager detects that the row refers to an extra file, the row from the extra file is loaded as well and given
	 * to the rowManager. Note that a row filled with only zeroes counts as not existing.
	 *
	 * @param resourceId
	 *            The desired resource
	 * @return True if the row was found in the index.
	 */
	private boolean loadRow(long resourceId) throws IOException {
//		SimpleLogger.log("----- ID " + resourceId);
		byte[] row = fileManager.readIndexRow(resourceId);
		if ((row == null) || Utils.isArrayZero(row)) {
			return false;
		}
		boolean dataExternal = rowManager.load(row);
//		SimpleLogger.log("row " + resourceId + ": " + Arrays.toString(rowManager.getRow()));
		if (dataExternal) {
			long fileId = rowManager.getFileId();
			long rowId = rowManager.getExternalFileRowId();
			byte[] dataBytes = fileManager.readExternalRow(fileId, rowId);
			if (dataBytes == null) {
				fileManager.readExternalRow(fileId, rowId);
				throw new RuntimeException("Row " + rowId + " not found in extra file " + fileId);
			}
			rowManager.loadExternalRow(dataBytes);
//			SimpleLogger.log("E (" + rowManager.getFileId() + ") Row: " + Arrays.toString(dataBytes));
		}
		return true;
	}

	private void insertEntryInExtraFile() throws IOException {
		long newExtraFileRowId = fileManager.writeExternalRow(rowManager.getFileId(), rowManager.getDataBytes());
		checkIfDataBytesLengthIsEnough(newExtraFileRowId);
//		SimpleLogger.log("I->E " + newExtraFileRowId + ": " + Arrays.toString(rowManager.getDataBytes()));
		rowManager.updateExtraRowId(newExtraFileRowId);
	}

	/**
	 * Flushes and closes the fileManager. Also resets internal dirty flag. Note: This method is not threadsafe
	 *
	 * @throws IOException
	 */
	public void defrag() throws IOException {
		// Stores file ids of all extra files that are defragged and therefore need to be exchanged by the temporary
		// files.
		Set<Long> defraggedFiles = new TreeSet<>();
		for (int resourceId = 1; resourceId <= getMaxId(); resourceId++) {
			if (!loadRow(resourceId)) {
				throw new RuntimeException("Empty row for resource " + resourceId + " found while defragging");
			}
			if (rowManager.isDataExternal()) {
				long fileId = rowManager.getFileId();
				// Use negative file ids for the temporary extra files. This is not only simple, but also ensures that
				// the maximal amount of open files (set in the FileManager) is adhered to.
				long newRowId = fileManager.writeExternalRow(-fileId, rowManager.getDataBytes());
				rowManager.updateExtraRowId(newRowId);
				defraggedFiles.add(fileId);
				fileManager.writeIndexRow(resourceId, rowManager.getRow());
			}
		}
		fileManager.defragFreeSpaceIndexes();
		// Close all StorageAccessors because we are deleting the files they have open.
		// Also, closing clears all caches that are incoherent to the changed files now.
		fileManager.close();
		dirty = false;
		for (Long fileId : defraggedFiles) {
			File oldFile = new File(statisticsDirPath + fileId);
			File newFile = new File(statisticsDirPath + (-fileId));
			oldFile.delete();
			newFile.renameTo(oldFile);
			fileManager.deleteExtraFile(-fileId);
		}
		// TODO: Is there a reason to ensure flushing afterwards?
	}

	/**
	 * Flushes triplesPerChunk data, and if the internal dirty flag is set, the FileManager.
	 */
	public void flush() {
		// Always flush triplesPerChunk because low cost
		byte[] bytes = new byte[Integer.BYTES + (Long.BYTES * triplesPerChunk.length)];
		NumberConversion.int2bytes(rowDataLength, bytes, 0);
		for (int i = 0; i < triplesPerChunk.length; i++) {
			NumberConversion.long2bytes(triplesPerChunk[i], bytes, Integer.BYTES + (i * Long.BYTES));
		}
		try {
			Files.write(statisticsMetadataFile, bytes);
			// Flush heavy data only if dirty
			if (!dirty) {
				return;
			}
			fileManager.flush();
			dirty = false;
		} catch (IOException e1) {
			throw new RuntimeException(e1);
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
		if ((Long.SIZE - Long.numberOfLeadingZeros(newRowId)) > (rowDataLength * Byte.SIZE)) {
			throw new RuntimeException(
					"There are too many extra file rows to be adressable with the current data bytes length. Please set rowDataLength parameter to a larger value.");
		}
	}

	/**
	 * Only works if all entries were written in this session
	 *
	 * @return
	 */
	public long getMaxId() {
		return fileManager.getMaxResourceId();
	}

	public int getRowDataLength() {
		return rowDataLength;
	}

	public long getIndexFileLength() {
		return fileManager.getIndexFileLength();
	}

	public Map<Long, Long> getFreeSpaceIndexLenghts() {
		return fileManager.getFreeSpaceIndexLengths();
	}

	@Override
	public void close() {
		flush();
		fileManager.close();
	}

	/**
	 * Collects and returns a formatted statistical report on all the written entries. Note that not all entries may be
	 * counted if the index file is not flushed beforehand.
	 *
	 * @return
	 * @throws IOException
	 */
	public String getDataStatistics() throws IOException {
		long maxId = getMaxId();
		for (long id = 1; id <= maxId; id++) {
			boolean rowFound = loadRow(id);
			if (!rowFound) {
				System.err.println("Missing Row for Ressource ID " + id);
				continue;
			}
			rowManager.collectStatistics();
		}
		return rowManager.getStatistics();
	}

	public Map<Long, long[]> getStorageStatistics() {
		return fileManager.getStorageStatistics();
	}

	public long getTotalEntries() {
		return rowManager.getTotalEntries();
	}

	public long getUnusedBytes() {
		return rowManager.getUnusedBytes();
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
