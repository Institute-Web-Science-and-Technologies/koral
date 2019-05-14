package playground;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatisticsDatabase;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.SingleFileGraphStatisticsDatabase;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.FileManager;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.MultiFileGraphStatisticsDatabase;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.Utils;

public class SingleToMultiStatisticsDBConverter {

	private static final boolean WRITE_STATISTICS_TO_CSV = false;

	private static void printUsage() {
		System.out.println("Usage: java " + SingleToMultiStatisticsDBConverter.class.getName()
				+ " <SingleFileDB storage directory> <number of chunks> [rowDataLength]");
	}

	public static void main(String[] args) throws IOException {
		String movedDir = "";

		if ((args.length != 2) && (args.length != 3)) {
			printUsage();
			return;
		}

		File oldStatisticsDir = new File(args[0]);
		if (!oldStatisticsDir.exists() || !oldStatisticsDir.isDirectory()) {
			System.err.println("Could not find directory " + args[0]);
			printUsage();
			return;
		}

		short numberOfChunks = -1;
		try {
			numberOfChunks = Short.parseShort(args[1]);
		} catch (NumberFormatException e) {
			System.err.println("Could not parse number of chunks parameter to integer: " + args[1]);
			printUsage();
			return;
		}

		int rowDataLength = -1;
		if (args.length == 3) {
			try {
				rowDataLength = Integer.parseInt(args[2]);
			} catch (NumberFormatException e) {
				System.err.println("Could not parse row data length parameter to integer: " + args[2]);
				printUsage();
				return;
			}
		}

		Configuration conf = new Configuration();
		File statisticsDir = new File(conf.getStatisticsDir(true));
		if (statisticsDir.equals(oldStatisticsDir)) {
			// Move old statistic dir to new tmp dir
			File tmpStatisticsDir = new File(System.getProperty("java.io.tmpdir") + File.separator + "oldStatistics");
			if (tmpStatisticsDir.exists()) {
				FileUtils.deleteDirectory(tmpStatisticsDir);
			}
			FileUtils.moveDirectory(oldStatisticsDir, tmpStatisticsDir);
			oldStatisticsDir = tmpStatisticsDir;
			statisticsDir.mkdirs();
			movedDir = "A copy of the old database can be found at " + tmpStatisticsDir.getCanonicalPath();
		}
		FileUtils.cleanDirectory(statisticsDir);

		SingleFileGraphStatisticsDatabase oldDatabase = new SingleFileGraphStatisticsDatabase(
				oldStatisticsDir.getCanonicalPath(), numberOfChunks);
		MultiFileGraphStatisticsDatabase newDatabase = null;
		if (rowDataLength >= 0) {
			newDatabase = new MultiFileGraphStatisticsDatabase(statisticsDir.getCanonicalPath(), numberOfChunks,
					rowDataLength, FileManager.DEFAULT_BLOCK_SIZE, true, FileManager.DEFAULT_INDEX_FILE_CACHE_SIZE,
					FileManager.DEFAULT_EXTRAFILES_CACHE_SIZE, FileManager.DEFAULT_RECYCLER_CAPACITY, FileManager.DEFAULT_MAX_OPEN_FILES, FileManager.DEFAULT_HABSE_ACCESSES_WEIGHT,
					FileManager.DEFAULT_HABSE_HISTORY_LENGTH, null);
		} else {
			newDatabase = new MultiFileGraphStatisticsDatabase(statisticsDir.getCanonicalPath(), numberOfChunks, null);
		}

		System.out.println("Copying entries...");

		long[] triplesPerChunk = oldDatabase.getChunkSizes();
		newDatabase.setNumberOfTriplesPerChunk(triplesPerChunk);

		long maxId = oldDatabase.getMaxId();
		for (int resourceId = 1; resourceId <= maxId; resourceId++) {
			long[] occurences = oldDatabase.getStatisticsForResource(resourceId);
			if (Utils.isArrayZero(occurences)) {
				System.err.println("Warning: Skipped empty row in old database");
				continue;
			}
			newDatabase.insertEntry(resourceId, occurences);
		}
		// For testing the correctness: comparing via diff
		if (WRITE_STATISTICS_TO_CSV) {
			System.out.println("Writing data to CSV...");
			writeStatisticsToCSV(new File("."), oldDatabase);
			writeStatisticsToCSV(new File("."), newDatabase);
		}
		// TODO: Diff automatically and return as check result

		oldDatabase.close();
		newDatabase.close();

		System.out.println("Finished. " + movedDir);
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

}
