package playground;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatisticsDatabase;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.SingleFileGraphStatisticsDatabase;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.CentralLogger;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.FileManager;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.MultiFileGraphStatisticsDatabase;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.SubbenchmarkManager;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching.DoublyLinkedNodeRecycler;

/**
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class StatisticsDBTest {

	/*
	 * Debug flags only used in this current class.
	 */

	private static final boolean WRITE_BENCHMARK_RESULTS = true;

	private static final boolean COLLECT_META_STATISTICS = false;

	private static final boolean WRITE_STATISTICS_DATA = false;

	/*
	 * Performance influencing flags. Making these constant allows removal of all related code at compile time
	 * optimization.
	 */

	public static final boolean ENABLE_STORAGE_LOGGING = false;

	public static final boolean SUBBENCHMARKS = false;

	public static final boolean WATCH_FILE_FLOW = false;

	public static final boolean LOG_SLRU_CACHE_SIZES = false;

	public static final boolean LOG_SLRU_CACHE_HITS = false;

	private static void printUsage() {
		System.out.println("Usage: java " + StatisticsDBTest.class.getName()
				+ " <encodedChunksDir> <logDir> <storageDir> <resultCSVFile> <implementation: single|multi> <rowDataLength> <indexCacheSizeMB> <extraFilesCacheSizeMB> [implementationNote]");
	}

	public static void main(String[] args) throws IOException {
		boolean fileLogging = true;
		if (args.length <= 1) {
			printUsage();
			return;
		}
		// argument counter for easier CLI parameter changes
		int argc = 0;
		File encodedChunksDir = new File(args[argc++]);
		if (!encodedChunksDir.exists() || !encodedChunksDir.isDirectory()) {
			System.err.println("Directory does not exist: " + encodedChunksDir);
			printUsage();
			return;
		}
		File logDir = new File(args[argc++]);
		if (!logDir.exists() || !logDir.isDirectory()) {
			System.err.println("Directory does not exist: " + logDir + ". Logging to file disabled.");
			fileLogging = false;
		}
		File storageDir = new File(args[argc++]);
		if (!storageDir.exists() || !storageDir.isDirectory()) {
			System.out.println("The path " + storageDir
					+ " is not a valid, existing directory. Database will be stored in temp dir.");
			storageDir = null;
		}
		File resultCSV = new File(args[argc++]);
		if (resultCSV.getParent() == null) {
			System.err.println("Invalid path for result file: " + resultCSV);
			printUsage();
			return;
		}
		File resultCSVDir = new File(resultCSV.getParent());
		if (!resultCSVDir.exists()) {
			resultCSVDir.mkdirs();
		}

		String datasetName = encodedChunksDir.getName();
		File[] encodedFiles = getEncodedChunkFiles(encodedChunksDir);
		short numberOfChunks = (short) encodedFiles.length;

		String implementation = args[argc++];
		System.out.println("Chosen implementation: " + implementation);
		int rowDataLength;
		long indexCacheSize, extraFilesCacheSize;
		String implementationNote = "";
		Configuration conf = new Configuration();
		if (args.length > argc) {
			rowDataLength = Integer.parseInt(args[argc++]);
			indexCacheSize = Long.parseLong(args[argc++]);
			extraFilesCacheSize = Long.parseLong(args[argc++]);
			if (args.length == (argc + 1)) {
				implementationNote = args[argc++];
			}
		} else {
			rowDataLength = conf.getRowDataLength();
			indexCacheSize = conf.getIndexCacheSize();
			extraFilesCacheSize = conf.getExtraCacheSize();
		}
		if (args.length > argc) {
			System.err.println("Too many arguments.");
			printUsage();
			return;
		}

		// Uncomment to simply store experimental parameters in a singleton class
//		SimpleConfiguration parameterConfig = SimpleConfiguration.getInstance();

		int blockSize = conf.getBlockSize();
		int recyclerCapacity = conf.getRecyclerCapacity();
		int maxOpenFiles = conf.getMaxOpenFiles();

		if (storageDir == null) {
			// Default to tmp dir
			storageDir = new File(conf.getStatisticsDir(true));
		}
		if (storageDir.exists() && (storageDir.listFiles().length > 0)) {
			System.err.println("WARNING: Given storage directory " + storageDir
					+ " is not empty. Content will be deleted in 5 seconds. Press Ctrl+C to abort.");
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e1) {
				throw new RuntimeException(e1);
			}
			try {
				FileUtils.cleanDirectory(storageDir);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		String[] datasetInfo = datasetName.split("_");
		String coveringAlgorithm = "NULL";
		int tripleCount = -1;
		short numberOfChunks_datasetName = -1;
		String configName = "";
		String configNameWithoutCaches = "";
		try {
			coveringAlgorithm = datasetInfo[0];
			numberOfChunks_datasetName = Short.parseShort(datasetInfo[1].replace("C", ""));
			if (numberOfChunks_datasetName != numberOfChunks) {
				System.err.println("Warning: Dataset name describes a partition count of " + numberOfChunks_datasetName
						+ " while there are " + numberOfChunks + " chunk files.");
			}
			tripleCount = Integer.parseInt(datasetInfo[2].replace("M", "000000").replace("K", "000"));
			configNameWithoutCaches = implementation + "_" + coveringAlgorithm + "_" + numberOfChunks + "C_"
					+ datasetInfo[2] + "T_" + rowDataLength + "DB";
			configName = configNameWithoutCaches + "_" + indexCacheSize + "IC_" + extraFilesCacheSize + "EC";
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

		System.out.println("Collecting Statistics...");

		GraphStatisticsDatabase statisticsDB = null;
		if (implementation.trim().equalsIgnoreCase("single")) {
			statisticsDB = new SingleFileGraphStatisticsDatabase(storageDir.getCanonicalPath(), numberOfChunks);
		} else if (implementation.trim().equalsIgnoreCase("multi")) {
			statisticsDB = new MultiFileGraphStatisticsDatabase(storageDir.getCanonicalPath(), numberOfChunks,
					rowDataLength, blockSize, true, indexCacheSize * 1024 * 1024L, extraFilesCacheSize * 1024 * 1024L,
					recyclerCapacity, maxOpenFiles, FileManager.DEFAULT_HABSE_ACCESSES_WEIGHT,
					FileManager.DEFAULT_HABSE_HISTORY_LENGTH, null);
		} else {
			System.err.println("Unknown implementation: " + implementation);
			return;
		}
		try (GraphStatistics statistics = new GraphStatistics(statisticsDB, numberOfChunks, null);) {
			final long operations = 3_000_000_000L;
			long start = System.currentTimeMillis();
			String benchmarkMode = "default";
			if (benchmarkMode.equals("default")) {
				statistics.collectStatistics(encodedFiles);
			} else if (benchmarkMode.equals("increment_one_resource")) {
				for (long i = 0; i < operations; i++) {
					statisticsDB.incrementSubjectCount(1, 0);
				}
			} else if (benchmarkMode.equals("increment_each_resource_once")) {
				for (long i = 0; i < operations; i++) {
					// Skip over half the rows each time
					long rid = 1 + (i / 2) + ((i % 2) == 0 ? 0 : operations / 2);
					statisticsDB.incrementSubjectCount(rid, 0);
				}
			} else if (benchmarkMode.equals("increment_column_wise")) {
				for (int r = 0; r < 3; r++) {
					for (int c = 0; c < numberOfChunks; c++) {
						for (long rid = 1; rid <= (operations / 3 / numberOfChunks); rid++) {
							switch (r) {
							case 0:
								statisticsDB.incrementSubjectCount(rid, c);
								break;
							case 1:
								statisticsDB.incrementPropertyCount(rid, c);
								break;
							case 2:
								statisticsDB.incrementObjectCount(rid, c);
								break;
							}
						}
					}
				}
			} else {
				throw new RuntimeException("Invalid benchmark mode");
			}
			long time = System.currentTimeMillis() - start;

			String timeFormatted = formatTime(time);
			long durationSec = time / 1_000;
//			System.out.println(statisticsDB);
			System.out.println("Collecting Statistics took " + timeFormatted);
			long totalInputReadTime = SubbenchmarkManager.getInstance().getInputReadTime() / (long) 1e9;

			long indexFileLength = -1;
			Map<Long, Long> freeSpaceIndexLengths = null;
			long totalEntries = -1;
			long unusedBytes = -1;
			long totalIndexFileTime = -1;
			long totalExtraFilesTime = -1;
			long totalCacheHits = 0, totalCacheMisses = 0, totalNotExisting = 0;
			double totalHitrate = 0;
			Map<Long, long[]> storageStatistics = null;
			if (statisticsDB instanceof MultiFileGraphStatisticsDatabase) {
				MultiFileGraphStatisticsDatabase multiDB = ((MultiFileGraphStatisticsDatabase) statisticsDB);
				if (ENABLE_STORAGE_LOGGING) {
					StorageLogWriter.getInstance().finish();
				}
				totalIndexFileTime = SubbenchmarkManager.getInstance().getIndexTime() / (long) 1e9;
				totalExtraFilesTime = SubbenchmarkManager.getInstance().getExtraTime() / (long) 1e9;
				CentralLogger.getInstance().finish(configName);
				SubbenchmarkManager.getInstance().finish(new File("subbenchmarks.csv"), configName, durationSec);
				DoublyLinkedNodeRecycler.getSegmentedLRUCacheRecycler().printStats();

				storageStatistics = multiDB.getStorageStatistics();
				for (Entry<Long, long[]> entry : storageStatistics.entrySet()) {
					long[] values = entry.getValue();
					totalCacheHits += values[0];
					totalCacheMisses += values[1];
					totalNotExisting += values[2];
				}
				totalHitrate = totalCacheHits / (double) (totalCacheHits + totalCacheMisses);

				freeSpaceIndexLengths = multiDB.getFreeSpaceIndexLenghts();
				System.out.println("Flushing database...");
				start = System.currentTimeMillis();
				multiDB.defrag();
				multiDB.flush();
				System.out.println("Flushing took " + formatTime(System.currentTimeMillis() - start));
				if (COLLECT_META_STATISTICS) {
					System.out.println("Collecting meta statistics...");
					writeDataStatisticsToFile(configNameWithoutCaches, multiDB.getDataStatistics());
				}
				indexFileLength = multiDB.getIndexFileLength();
				totalEntries = multiDB.getTotalEntries();
				unusedBytes = multiDB.getUnusedBytes();
			}
			long dirSize = dirSize(storageDir.getCanonicalPath());
			System.out.println("Dir Size: " + String.format("%,d", dirSize) + " Bytes");
			System.out.println("Index File size: " + String.format("%,d", indexFileLength) + " Bytes");
			if (WRITE_BENCHMARK_RESULTS) {
				writeStorageStatisticsToCSV(configName, storageDir.getCanonicalPath(), storageStatistics,
						totalCacheHits,
						totalCacheMisses, totalNotExisting, totalHitrate);
				System.out.println("Writing benchmarks to CSV...");
				SimpleDateFormat dateFormatter = new SimpleDateFormat("dd.MM.yy HH:mm");
				String date = dateFormatter.format(new Date());
				long extraFilesSize = getExtraFilesSize(storageDir);
				int extraFilesCount = 0;
				for (Long extraFileLength : freeSpaceIndexLengths.values()) {
					if (extraFileLength > 0) {
						extraFilesCount++;
					}
				}
				writeBenchmarkToCSV(resultCSV, date, tripleCount, numberOfChunks, rowDataLength, indexCacheSize,
						extraFilesCacheSize, implementation, coveringAlgorithm, implementationNote, durationSec,
						totalInputReadTime, totalIndexFileTime, totalExtraFilesTime, totalCacheHits,
						totalCacheMisses, totalHitrate, dirSize, indexFileLength,
						extraFilesSize, extraFilesCount, totalEntries, unusedBytes);
				System.out.println("Writing file distribution to CSV...");
				writeFileDistributionToCSV(configNameWithoutCaches, conf.getStatisticsDir(true), freeSpaceIndexLengths);
			}
			if (WRITE_STATISTICS_DATA) {
				// Read statistics and write into csv
				System.out.println("Writing statistics to file...");
				writeStatisticsToCSV(encodedChunksDir, statisticsDB);
			}
		}
		System.out.println("Finished at " + new Date() + ".");

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

	private static void writeBenchmarkToCSV(File resultFile, Object... row)
			throws UnsupportedEncodingException, FileNotFoundException, IOException {
		CSVFormat csvFileFormat = CSVFormat.RFC4180.withRecordSeparator('\n');
		CSVPrinter printer = new CSVPrinter(new OutputStreamWriter(new FileOutputStream(resultFile, true), "UTF-8"),
				csvFileFormat);
		if (resultFile.length() == 0) {
			printer.printRecord("DATE_FINISHED", "TRIPLES", "CHUNKS", "ROW_DATA_LENGTH", "INDEX_CACHE_MB",
					"EXTRAFILES_CACHE_MB", "DB_IMPL", "COV_ALG", "NOTE", "DURATION_SEC", "INPUT_TIME", "INDEX_TIME",
					"EXTRA_TIME", "TOTAL_CACHE_HITS", "TOTAL_CACHE_MISSES", "TOTAL_HITRATE", "DIR_SIZE_BYTES",
					"INDEX_SIZE_BYTES", "EXTRAFILES_SIZE_BYTES", "EXTRAFILES_COUNT", "TOTAL_ENTRIES",
					"UNUSED_BYTES");
		}
		printer.printRecord(row);
		printer.close();
	}

	private static void writeDataStatisticsToFile(String configNameWithoutCaches, String statistics)
			throws FileNotFoundException {
		System.out.println("Writing meta statistics to file...");
		try (PrintWriter writer = new PrintWriter("metastatistics-" + configNameWithoutCaches + ".txt")) {
			writer.write(statistics);
		}
	}

	private static void writeFileDistributionToCSV(String configName, String extraFilesDir,
			Map<Long, Long> freeSpaceIndexLengths) {
		try {
			CSVFormat csvFileFormat = CSVFormat.RFC4180.withRecordSeparator('\n');
			CSVPrinter printer = new CSVPrinter(
					new OutputStreamWriter(new FileOutputStream("fileDistribution-" + configName + ".csv"), "UTF-8"),
					csvFileFormat);
			if (freeSpaceIndexLengths != null) {
				printer.printRecord("FILE_ID", "FREESPACEINDEX_LENGTH", "SIZE_IN_BYTES");
				for (Entry<Long, Long> entry : freeSpaceIndexLengths.entrySet()) {
					long fileId = entry.getKey();
					File extraFile = new File(extraFilesDir + "/" + fileId);
					long fileSize = extraFile.length();
					printer.printRecord(fileId, entry.getValue(), fileSize);
				}
			}
			printer.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	private static void writeStatisticsToCSV(File outputDir, GraphStatisticsDatabase statisticsDB) {
		long maxId = 0;
		if (statisticsDB instanceof SingleFileGraphStatisticsDatabase) {
			maxId = ((SingleFileGraphStatisticsDatabase) statisticsDB).getMaxId();
		} else if (statisticsDB instanceof MultiFileGraphStatisticsDatabase) {
			maxId = ((MultiFileGraphStatisticsDatabase) statisticsDB).getMaxId();
		}
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
				// SingleDB has another zero entry per column for total resources, add that one
				// as well for easier
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

	private static long getExtraFilesSize(File storageDir) {
		long totalSize = 0;
		FilenameFilter fileNameNumberFilter = new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return StringUtils.isNumeric(name);
			}
		};
		for (File extraFile : storageDir.listFiles(fileNameNumberFilter)) {
			totalSize += extraFile.length();
		}
		return totalSize;
	}

	/**
	 * Attempts to calculate the size of a file or directory.
	 *
	 * <p>
	 * Since the operation is non-atomic, the returned value may be inaccurate. However, this method is quick and does
	 * its best.
	 */
	public static long dirSize(String pathString) {

		Path path = FileSystems.getDefault().getPath(pathString);

		final AtomicLong size = new AtomicLong(0);

		try {
			Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {

					size.addAndGet(attrs.size());
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult visitFileFailed(Path file, IOException exc) {

					System.err.println("skipped: " + file + " (" + exc + ")");
					// Skip folders that can't be traversed
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult postVisitDirectory(Path dir, IOException exc) {

					if (exc != null) {
						System.err.println("had trouble traversing: " + dir + " (" + exc + ")");
					}
					// Ignore errors traversing a folder
					return FileVisitResult.CONTINUE;
				}
			});
		} catch (IOException e) {
			throw new AssertionError("walkFileTree will not throw IOException if the FileVisitor does not");
		}

		return size.get();
	}

	public static String formatTime(long milliseconds) {
		return String.format("%d min, %d sec, %d ms", TimeUnit.MILLISECONDS.toMinutes(milliseconds),
				TimeUnit.MILLISECONDS.toSeconds(milliseconds)
						- TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(milliseconds)),
				milliseconds - TimeUnit.SECONDS.toMillis(TimeUnit.MILLISECONDS.toSeconds(milliseconds)));
	}

}
