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
//				System.out.println(statisticsDB);
			System.out.println("Collecting Statistics took " + timeFormatted);

			long dirSize = dirSize(conf.getStatisticsDir(true));
			Map<Long, Long> freeSpaceIndexLengths = null;
			long indexFileLength = -1;
			if (statisticsDB instanceof MultiFileGraphStatisticsDatabase) {
				freeSpaceIndexLengths = ((MultiFileGraphStatisticsDatabase) statisticsDB).getFreeSpaceIndexLenghts();
				indexFileLength = ((MultiFileGraphStatisticsDatabase) statisticsDB).getIndexFileLength();
			}
			writeBenchmarkResultsToCSV(datasetName, implementation, numberOfChunks, time, dirSize, indexFileLength,
					freeSpaceIndexLengths);
			// Read statistics and write into csv
//			System.out.println("Writing statistics to file...");
//			writeStatisticsToCSV(encodedChunksDir, statisticsDB);

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		System.out.println("Finished.");

	}

	private static void writeBenchmarkResultsToCSV(String datasetName, String implementation, short numberOfChunks,
			long time, long dirSize, long indexFileLength, Map<Long, Long> freeSpaceIndexLengths) {
		try {
			CSVFormat csvFileFormat = CSVFormat.RFC4180.withRecordSeparator('\n');
			CSVPrinter printer = new CSVPrinter(new OutputStreamWriter(
					new FileOutputStream(datasetName + "_" + implementation + "_" + numberOfChunks + "_chunks.csv"),
					"UTF-8"), csvFileFormat);
			printer.printRecord("TIME IN MS", time);
			printer.printRecord("DIR SIZE IN BYTES", dirSize);
			printer.printRecord("INDEX SIZE IN BYTES", indexFileLength);
			if (freeSpaceIndexLengths != null) {
				for (Entry<Long, Long> entry : freeSpaceIndexLengths.entrySet()) {
					printer.printRecord(entry.getKey(), entry.getValue());
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
			for (int id = 1; id < maxId; id++) {
				for (long l : statisticsDB.getStatisticsForResource(id)) {
					printer.print(l);
				}
				// SingleDB has another zero entry for total resources, add that one as well for easier diffing
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
