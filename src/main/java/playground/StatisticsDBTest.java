package playground;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Comparator;
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

	private static final boolean WRITE_BENCHMARK_RESULTS = false;

	private static final boolean COLLECT_META_STATISTICS = false;

	private static final boolean WRITE_STATISTICS_DATA = true;

	public static void main(String[] args) throws FileNotFoundException {

		if (args.length != 3) {
			System.out.println("Usage: java " + StatisticsDBTest.class.getName()
					+ " <encodedChunksDir> <numberOfChunks> <implementation: single|multi>");
			return;
		}
		File encodedChunksDir = new File(args[0]);
		if (!encodedChunksDir.exists() || !encodedChunksDir.isDirectory()) {
			System.err.println("Directory does not exist: " + encodedChunksDir);
		}
		String datasetName = encodedChunksDir.getName();
		File[] encodedFiles = encodedChunksDir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.contains("chunk") && name.endsWith(".enc.gz");
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
		short numberOfChunks = Short.parseShort(args[1]);
		String implementation = args[2];
		System.out.println("Chosen implementation: " + implementation);

		Configuration conf = new Configuration();

		System.out.println("Collecting Statistics...");

		File statisticsDir = new File(conf.getStatisticsDir(true));
		if (statisticsDir.exists()) {
			try {
				FileUtils.cleanDirectory(statisticsDir);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		GraphStatisticsDatabase statisticsDB = null;
		if (implementation.trim().equalsIgnoreCase("single")) {
			statisticsDB = new SingleFileGraphStatisticsDatabase(conf.getStatisticsDir(true), numberOfChunks);
		} else if (implementation.trim().equalsIgnoreCase("multi")) {
			statisticsDB = new MultiFileGraphStatisticsDatabase(conf.getStatisticsDir(true), numberOfChunks);
		} else {
			System.err.println("Unknown implementation: " + implementation);
			return;
		}
		try (GraphStatistics statistics = new GraphStatistics(statisticsDB, numberOfChunks, null);) {
			long start = System.currentTimeMillis();
			statistics.collectStatistics(encodedFiles);
			long time = System.currentTimeMillis() - start;

			String timeFormatted = String.format("%d min, %d sec, %d ms", TimeUnit.MILLISECONDS.toMinutes(time),
					TimeUnit.MILLISECONDS.toSeconds(time)
							- TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(time)),
					time - TimeUnit.SECONDS.toMillis(TimeUnit.MILLISECONDS.toSeconds(time)));
//			System.out.println(statisticsDB);
			System.out.println("Collecting Statistics took " + timeFormatted);

			long indexFileLength = -1;
			Map<Long, Long> freeSpaceIndexLengths = null;
			if (statisticsDB instanceof MultiFileGraphStatisticsDatabase) {
				MultiFileGraphStatisticsDatabase multiDB = ((MultiFileGraphStatisticsDatabase) statisticsDB);
				multiDB.flush();
				if (COLLECT_META_STATISTICS) {
					System.out.println(multiDB.getStatistics());
				}
				freeSpaceIndexLengths = multiDB.getFreeSpaceIndexLenghts();
				indexFileLength = multiDB.getIndexFileLength();
			}
			long dirSize = dirSize(conf.getStatisticsDir(true));
			System.out.println("Dir Size: " + String.format("%,d", dirSize) + " Bytes");
			System.out.println("Index File size: " + String.format("%,d", indexFileLength) + " Bytes");
			if (WRITE_BENCHMARK_RESULTS) {
				writeBenchmarkResultsToCSV(datasetName, implementation, numberOfChunks, conf.getStatisticsDir(true),
						freeSpaceIndexLengths);
			}
			if (WRITE_STATISTICS_DATA) {
				// Read statistics and write into csv
				System.out.println("Writing statistics to file...");
				writeStatisticsToCSV(encodedChunksDir, statisticsDB);
			}
		}
		System.out.println("Finished.");

	}

	private static void writeBenchmarkResultsToCSV(String datasetName, String implementation, short numberOfChunks,
			String extraFilesDir, Map<Long, Long> freeSpaceIndexLengths) {
		try {
			CSVFormat csvFileFormat = CSVFormat.RFC4180.withRecordSeparator('\n');
			CSVPrinter printer = new CSVPrinter(new OutputStreamWriter(
					new FileOutputStream(datasetName + "_" + implementation + "_" + numberOfChunks + "_chunks.csv"),
					"UTF-8"), csvFileFormat);
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

}
