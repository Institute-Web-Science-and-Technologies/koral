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

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatisticsDatabase;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.SingleFileGraphStatisticsDatabase;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.MultiFileGraphStatisticsDatabase;

/**
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class StatisticsDBTest {

	private static final boolean WRITE_BENCHMARK_RESULTS = true;

	private static final boolean COLLECT_META_STATISTICS = true;

	private static final boolean WRITE_STATISTICS_DATA = true;

	private static void printUsage() {
		System.out.println("Usage: java " + StatisticsDBTest.class.getName()
				+ " <encodedChunksDir> <logDir> <storageDir> <resultCSVFile> <implementation: single|multi> <rowDataLength> <indexCacheSizeMB> <extraFilesCacheSizeMB> [implementationNote]");
	}

	public static void main(String[] args) throws IOException {
		boolean fileLogging = true;
		if ((args.length != 8) && (args.length != 9)) {
			System.err.println("Invalid amount of arguments.");
			printUsage();
			return;
		}
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
			System.out.println("The path " + storageDir
					+ " is not a valid, exisiting directory. Database will be stored in temp dir.");
			storageDir = null;
		} else if (storageDir.list().length > 0) {
			// We don't want to be responsible for deleting directory content
			System.err.println("The given storage directory " + storageDir + " is not empty.");
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

		String datasetName = encodedChunksDir.getName();
		File[] encodedFiles = getEncodedChunkFiles(encodedChunksDir);
		short numberOfChunks = (short) encodedFiles.length;

		String implementation = args[4];
		System.out.println("Chosen implementation: " + implementation);
		int rowDataLength = Integer.parseInt(args[5]);
		long indexCacheSize = Long.parseLong(args[6]);
		long extraFilesCacheSize = Long.parseLong(args[7]);
		String implementationNote = "";
		if (args.length == 9) {
			implementationNote = args[8];
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

		Configuration conf = new Configuration();
		System.out.println("Collecting Statistics...");

		if (storageDir == null) {
			// Default to tmp dir
			storageDir = new File(conf.getStatisticsDir(true));
			if (storageDir.exists()) {
				try {
					FileUtils.cleanDirectory(storageDir);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		GraphStatisticsDatabase statisticsDB = null;
		if (implementation.trim().equalsIgnoreCase("single")) {
			statisticsDB = new SingleFileGraphStatisticsDatabase(storageDir.getCanonicalPath(), numberOfChunks);
		} else if (implementation.trim().equalsIgnoreCase("multi")) {
			statisticsDB = new MultiFileGraphStatisticsDatabase(storageDir.getCanonicalPath(), numberOfChunks,
					rowDataLength, indexCacheSize * 1024 * 1024L, extraFilesCacheSize * 1024 * 1024L, null);
		} else {
			System.err.println("Unknown implementation: " + implementation);
			return;
		}
		try (GraphStatistics statistics = new GraphStatistics(statisticsDB, numberOfChunks, null);) {
			long start = System.currentTimeMillis();
			statistics.collectStatistics(encodedFiles);
			long time = System.currentTimeMillis() - start;

			String timeFormatted = formatTime(time);
			long durationSec = time / 1_000;
//			System.out.println(statisticsDB);
			System.out.println("Collecting Statistics took " + timeFormatted);

			long indexFileLength = -1;
			Map<Long, Long> freeSpaceIndexLengths = null;
			long totalEntries = -1;
			long unusedBytes = -1;
			if (statisticsDB instanceof MultiFileGraphStatisticsDatabase) {
				MultiFileGraphStatisticsDatabase multiDB = ((MultiFileGraphStatisticsDatabase) statisticsDB);
				System.out.println("Flushing database...");
				start = System.currentTimeMillis();
				multiDB.defrag();
				multiDB.flush();
				System.out.println("Flushing took " + formatTime(System.currentTimeMillis() - start));
				if (COLLECT_META_STATISTICS) {
					System.out.println("Collecting meta statistics...");
					writeMetaStatisticsToFile(configNameWithoutCaches, multiDB.getStatistics());
				}
				freeSpaceIndexLengths = multiDB.getFreeSpaceIndexLenghts();
				indexFileLength = multiDB.getIndexFileLength();
				totalEntries = multiDB.getTotalEntries();
				unusedBytes = multiDB.getUnusedBytes();
			}
			long dirSize = dirSize(storageDir.getCanonicalPath());
			System.out.println("Dir Size: " + String.format("%,d", dirSize) + " Bytes");
			System.out.println("Index File size: " + String.format("%,d", indexFileLength) + " Bytes");
			if (WRITE_BENCHMARK_RESULTS) {
				System.out.println("Writing benchmarks to CSV...");
				// For the extra file size the difference of dir and index size is calculated. For
				// 1000M dataset, deviation is less than 0.001%
				writeBenchmarkToCSV(resultCSV, tripleCount, numberOfChunks, rowDataLength, indexCacheSize,
						extraFilesCacheSize, implementation, coveringAlgorithm, implementationNote, durationSec,
						dirSize, indexFileLength, dirSize - indexFileLength, totalEntries, unusedBytes);
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

	private static void writeBenchmarkToCSV(File resultFile, int tripleCount, short numberOfChunks, int dataBytes,
			long indexCacheSize, long extraFilesCacheSize, String dbImplementation, String coveringAlgorithm,
			String implementationNote, long durationSec, long dirSizeBytes, long indexSizeBytes,
			long extraFilesSizeBytes, long totalEntries, long unusedBytes)
			throws UnsupportedEncodingException, FileNotFoundException, IOException {
		CSVFormat csvFileFormat = CSVFormat.RFC4180.withRecordSeparator('\n');
		CSVPrinter printer = new CSVPrinter(new OutputStreamWriter(new FileOutputStream(resultFile, true), "UTF-8"),
				csvFileFormat);
		if (resultFile.length() == 0) {
			// The extra file size is only approximate, because only the difference of dir and index size is calculated.
			// For 1000M dataset, deviation is less than 0.001%
			printer.printRecord("TRIPLES", "CHUNKS", "ROW_DATA_LENGTH", "INDEX_CACHE_MB", "EXTRAFILES_CACHE_MB",
					"DB_IMPL", "COV_ALG", "NOTE", "DURATION_SEC", "DIR_SIZE_BYTES", "INDEX_SIZE_BYTES",
					"APPROX_EXTRAFILES_SIZE_BYTES", "TOTAL_ENTRIES", "UNUSED_BYTES");
		}
		printer.printRecord(tripleCount, numberOfChunks, dataBytes, indexCacheSize, extraFilesCacheSize,
				dbImplementation, coveringAlgorithm, implementationNote, durationSec, dirSizeBytes, indexSizeBytes,
				extraFilesSizeBytes, totalEntries, unusedBytes);
		printer.close();
	}

	private static void writeMetaStatisticsToFile(String configNameWithoutCaches, String statistics)
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
