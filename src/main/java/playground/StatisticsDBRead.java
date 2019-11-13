package playground;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatisticsDatabase;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.FileManager;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.MultiFileGraphStatisticsDatabase;

public class StatisticsDBRead {

	private static final boolean WRITE_STATISTICS_DATA = false;

	private static void printUsage() {
		System.out.println(
				"Usage: java -jar StatisticsDBRead.jar <encodedChunksDir> <logDir> <storageDir> <resultCSVFile> <indexCacheSizeMB> <extraFilesCacheSizeMB> <readMode> [implementationNote]");
	}

	public static void main(String[] args) throws IOException {
		boolean fileLogging = true;
		File encodedChunksDir = new File(args[0]);
		if (!encodedChunksDir.exists() || !encodedChunksDir.isDirectory()) {
			System.err.println("Directory does not exist: " + encodedChunksDir);
			printUsage();
			return;
		}

		File logDir = new File(args[1]);
		if (!logDir.exists() || !logDir.isDirectory()) {
			System.err.println("Directory does not exist: " + logDir + ". Logging to file disabled.");
			fileLogging = false;
		}

		File storageDir = new File(args[2]);
		if (!storageDir.exists() || !storageDir.isDirectory()) {
			System.out.println("The path " + storageDir + " is not a valid, exisiting directory.");
			printUsage();
			return;
		} else if (storageDir.list().length == 0) {
			System.err.println("The given storage directory " + storageDir + " is empty.");
			return;
		}

		File resultCSV = new File(args[3]);
		if (resultCSV.getParent() == null) {
			System.err.println("Invalid path for result file: " + args[3]);
			printUsage();
			return;
		}
		File resultCSVDir = new File(resultCSV.getParent());
		if (!resultCSVDir.exists()) {
			resultCSVDir.mkdirs();
		}

		long indexCacheSize = Long.parseLong(args[4]);
		long extraFilesCacheSize = Long.parseLong(args[5]);
		String readMode = args[6];
		String implementationNote = "";
		if (args.length == 8) {
			implementationNote = args[7];
		}

		String datasetName = encodedChunksDir.getName();
		File[] encodedFiles = getEncodedChunkFiles(encodedChunksDir);
		short numberOfChunks = (short) encodedFiles.length;

		String[] datasetInfo = datasetName.split("_");
		String coveringAlgorithm = "NULL";
		int tripleCount = -1;
		short numberOfChunks_datasetName = -1;
		String configName = "";
		try {
			coveringAlgorithm = datasetInfo[0];
			numberOfChunks_datasetName = Short.parseShort(datasetInfo[1].replace("C", ""));
			if (numberOfChunks_datasetName != numberOfChunks) {
				System.err.println("Warning: Dataset name describes a partition count of " + numberOfChunks_datasetName
						+ " while there are " + numberOfChunks + " chunk files.");
			}
			tripleCount = Integer.parseInt(datasetInfo[2].replace("M", "000000").replace("K", "000"));
			configName = "read_" + coveringAlgorithm + "_" + numberOfChunks + "C_" + datasetInfo[2] + "T_"
					+ indexCacheSize + "IC_" + extraFilesCacheSize + "EC_" + readMode;
			configName += implementationNote.isEmpty() ? "" : "_" + implementationNote.replace(" ", "-");
		} catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
			System.err.println(
					"Unknown directory name format, please use [CoverAlgorithm]_[Chunks]C_[Triples][K/M]. Benchmark CSV will now be filled with NULLs.");
		}

		if (!configName.equals("") && fileLogging) {
			File logFile = new File(logDir.getCanonicalPath() + File.separator + configName + ".log");
			System.out.println("Redirecting stdout and stderr to " + logFile.getCanonicalPath() + " now.");
			PrintStream out = new PrintStream(new BufferedOutputStream(new FileOutputStream(logFile)), true);
			System.setOut(out);
			System.setErr(out);
		}

		System.out.println("Starting at " + new Date());
		System.out.println("Config string: " + configName);
		System.out.println("Reading Statistics...");

		MultiFileGraphStatisticsDatabase statisticsDB = new MultiFileGraphStatisticsDatabase(
				storageDir.getCanonicalPath(), numberOfChunks, -99, FileManager.DEFAULT_BLOCK_SIZE, true,
				indexCacheSize * 1024 * 1024L, extraFilesCacheSize * 1024 * 1024L,
				FileManager.DEFAULT_RECYCLER_CAPACITY, FileManager.DEFAULT_MAX_OPEN_FILES,
				FileManager.DEFAULT_HABSE_ACCESSES_WEIGHT, FileManager.DEFAULT_HABSE_HISTORY_LENGTH, null);

		long optimizationPreventer = 0;
		Random random = new Random();
		GraphStatistics statistics = new GraphStatistics(statisticsDB, numberOfChunks, null);

		long start = System.currentTimeMillis();
		if (readMode.equals("DATASET")) {
			for (int i = 0; i < encodedFiles.length; i++) {
				EncodedFileInputStream in = new EncodedFileInputStream(EncodingFileFormat.EEE, encodedFiles[i]);
				for (Statement statement : in) {
					optimizationPreventer += statistics.getTotalSubjectFrequency(statement.getSubjectAsLong());
					optimizationPreventer += statistics.getTotalPropertyFrequency(statement.getPropertyAsLong());
					optimizationPreventer += statistics.getTotalObjectFrequency(statement.getObjectAsLong());
				}
				in.close();
				System.out.println("Chunk " + i + " done.");
			}
		} else if (readMode.equals("SEQUENTIAL_ROWS_SEQUENTIAL_COLUMNS")) {
			for (long r = 1; r <= statisticsDB.getMaxId(); r++) {
				for (int c = 0; c < (3 * numberOfChunks); c++) {
					optimizationPreventer += readTableCell(statistics, r, c);
				}
			}
		} else if (readMode.equals("SEQUENTIAL_ROWS_RANDOM_COLUMNS")) {
			int columnAccesses = 30;
			for (long r = 1; r <= statisticsDB.getMaxId(); r++) {
				for (int access = 0; access < columnAccesses; access++) {
					int c = random.nextInt(3 * numberOfChunks);
					optimizationPreventer += readTableCell(statistics, r, c);
				}
			}
		} else if (readMode.equals("RANDOM_ROWS_SEQUENTIAL_COLUMNS")) {
			long rowAccesses = statisticsDB.getMaxId();
			for (long access = 0; access < rowAccesses; access++) {
				long r = (long) (random.nextDouble() * statisticsDB.getMaxId()) + 1;
				for (int c = 0; c < (3 * numberOfChunks); c++) {
					optimizationPreventer += readTableCell(statistics, r, c);
				}
			}

		} else if (readMode.equals("RANDOM_ROWS_RANDOM_COLUMNS")) {
			long rowAccesses = statisticsDB.getMaxId();
			int columnAccesses = 30;
			for (long rowAccess = 0; rowAccess < rowAccesses; rowAccess++) {
				long r = (long) (random.nextDouble() * statisticsDB.getMaxId()) + 1;
				for (int colAccess = 0; colAccess < columnAccesses; colAccess++) {
					int c = random.nextInt(3 * numberOfChunks);
					optimizationPreventer += readTableCell(statistics, r, c);
				}
			}
		} else {
			System.err.println("Invalid read mode: " + readMode);
			System.exit(1);
		}
		long durationSec = (System.currentTimeMillis() - start) / 1000;
		System.out.println("Reading took " + durationSec + " sec");

		System.out.println(optimizationPreventer);

		System.out.println("Writing benchmark results to CSV...");
		Map<Long, long[]> storageStatistics = statisticsDB.getStorageStatistics();
		long totalCacheHits = 0, totalCacheMisses = 0, totalNotExisting = 0;
		for (Entry<Long, long[]> entry : storageStatistics.entrySet()) {
			long[] values = entry.getValue();
			totalCacheHits += values[0];
			totalCacheMisses += values[1];
			totalNotExisting += values[2];
		}
		double totalHitrate = totalCacheHits / (double) (totalCacheHits + totalCacheMisses);

		writeStorageStatisticsToCSV(configName, storageDir.getCanonicalPath(), storageStatistics, totalCacheHits,
				totalCacheMisses, totalNotExisting, totalHitrate);

		writeBenchmarkToCSV(resultCSV, tripleCount, numberOfChunks, readMode, statisticsDB.getRowDataLength(),
				indexCacheSize, extraFilesCacheSize, coveringAlgorithm, implementationNote, durationSec, totalCacheHits,
				totalCacheMisses, totalHitrate);

		if (WRITE_STATISTICS_DATA) {
			// Read statistics and write into csv
			System.out.println("Writing statistics to file...");
			writeStatisticsToCSV(encodedChunksDir, statisticsDB, statisticsDB.getMaxId());
		}

		statistics.close();
		System.out.println("Finished at " + new Date());
	}

	private static File[] getEncodedChunkFiles(File encodedChunksDir) {
		File[] encodedFiles = encodedChunksDir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.contains("chunk") && name.endsWith(".gz");
			}
		});
		Arrays.sort(encodedFiles, new Comparator<File>() {
			@Override
			public int compare(File file1, File file2) {
				int chunkIndex1 = Integer.parseInt(file1.getName().split("\\.")[0].replace("chunk", ""));
				int chunkIndex2 = Integer.parseInt(file2.getName().split("\\.")[0].replace("chunk", ""));
				return Integer.compare(chunkIndex1, chunkIndex2);
			}
		});
		System.out.println("Recognized chunk files:");
		for (File file : encodedFiles) {
			System.out.println(file);
		}
		return encodedFiles;
	}

	private static long readTableCell(GraphStatistics statistics, long row, int col) {
		int t = col / 3;
		int chunk = col % 3;
		switch (t) {
		case 0:
			return statistics.getSubjectFrequency(row, chunk);
		case 1:
			return statistics.getPropertyFrequency(row, chunk);
		case 2:
			return statistics.getObjectFrequency(row, chunk);
		}
		return 0L;
	}

	private static void writeBenchmarkToCSV(File resultFile, int tripleCount, int numberOfChunks, String readMode,
			int dataBytes, long indexCacheSize, long extraFilesCacheSize, String coveringAlgorithm,
			String implementationNote, long durationSec, long totalCacheHits, long totalCacheMisses,
			double totalHitrate) throws UnsupportedEncodingException, FileNotFoundException, IOException {
		CSVFormat csvFileFormat = CSVFormat.RFC4180.withRecordSeparator('\n');
		CSVPrinter printer = new CSVPrinter(new OutputStreamWriter(new FileOutputStream(resultFile, true), "UTF-8"),
				csvFileFormat);
		if (resultFile.length() == 0) {
			printer.printRecord("TRIPLES", "CHUNKS", "READMODE", "ROW_DATA_LENGTH", "INDEX_CACHE_MB",
					"EXTRAFILES_CACHE_MB", "COV_ALG", "NOTE", "DURATION_SEC", "CACHE_HITS", "CACHE_MISSES",
					"CACHE_HITRATE");
		}
		printer.printRecord(tripleCount, numberOfChunks, readMode, dataBytes, indexCacheSize, extraFilesCacheSize,
				coveringAlgorithm, implementationNote, durationSec, totalCacheHits, totalCacheMisses, totalHitrate);
		printer.close();
	}

	private static void writeStorageStatisticsToCSV(String configName, String storageDir,
			Map<Long, long[]> storageStatistics, long totalCacheHits, long totalCacheMisses, long totalNotExisting,
			double totalHitrate) throws IOException {
		CSVFormat csvFileFormat = CSVFormat.RFC4180.withRecordSeparator('\n');
		CSVPrinter printer = new CSVPrinter(new OutputStreamWriter(
				new FileOutputStream("storageStatistics-" + configName + ".csv", false), "UTF-8"), csvFileFormat);
		printer.printRecord("FILE_ID", "FILE_SIZE", "CACHE_HITS", "CACHE_MISSES", "CACHE_HITRATE", "NOT_EXISTING");
		for (Entry<Long, long[]> entry : storageStatistics.entrySet()) {
			long fileId = entry.getKey();
			long[] values = entry.getValue();
			File file;
			if (fileId == 0) {
				file = new File(storageDir, "statistics");
			} else {
				file = new File(storageDir, String.valueOf(fileId));
			}
			double hitrate = values[0] / (double) (values[0] + values[1]);
			printer.printRecord(fileId, file.length(), values[0], values[1], String.format("%.3f", hitrate), values[2]);
		}
		printer.println();
		printer.printRecord("TOTAL", "", totalCacheHits, totalCacheMisses, String.format("%.3f", totalHitrate),
				totalNotExisting);
		printer.close();

	}

	/**
	 * Writes the collected tabular data into a csv. Only used for debugging.
	 *
	 * @param outputDir
	 * @param statisticsDB
	 */
	private static void writeStatisticsToCSV(File outputDir, GraphStatisticsDatabase statisticsDB, long maxId) {
		try {
			CSVFormat csvFileFormat = CSVFormat.RFC4180.withRecordSeparator('\n');
			CSVPrinter printer = new CSVPrinter(new OutputStreamWriter(new FileOutputStream(outputDir.getCanonicalPath()
					+ File.separator + statisticsDB.getClass().getSimpleName() + "-statistics.csv"), "UTF-8"),
					csvFileFormat);
			for (long l : statisticsDB.getChunkSizes()) {
				printer.print(l);
			}
			printer.println();
			for (int id = 1; id <= maxId; id++) {
				for (long l : statisticsDB.getStatisticsForResource(id)) {
					printer.print(l);
				}
				// SingleDB has another zero entry per column for total resources, add that one as well for easier
				// diffing
				if (statisticsDB instanceof MultiFileGraphStatisticsDatabase) {
					printer.print(0);
				}
				printer.println();
			}
			printer.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
