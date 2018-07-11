package playground;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatisticsDatabase;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.MultiFileGraphStatisticsDatabase;

public class StatisticsDBRead {

	private static final boolean WRITE_STATISTICS_DATA = false;

	private static void printUsage() {
		System.out.println(
				"Usage: java -jar StatisticsDBRead.jar <encodedChunksDir> <logDir> <storageDir> <resultCSVFile> <indexCacheSizeMB> <extraFilesCacheSizeMB> [implementationNote]");
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
		String implementationNote = "";
		if (args.length == 7) {
			implementationNote = args[6];
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
					+ indexCacheSize + "IC_" + extraFilesCacheSize + "EC";
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

		GraphStatisticsDatabase statisticsDB = new MultiFileGraphStatisticsDatabase(storageDir.getCanonicalPath(),
				numberOfChunks, -99, indexCacheSize * 1024 * 1024L, extraFilesCacheSize * 1024 * 1024L, null);

		long optimizationPreventer = 0;
		GraphStatistics statistics = new GraphStatistics(statisticsDB, numberOfChunks, null);
		long start = System.currentTimeMillis();
		for (int i = 0; i < encodedFiles.length; i++) {
			EncodedFileInputStream in = new EncodedFileInputStream(EncodingFileFormat.EEE, encodedFiles[i]);
			for (Statement statement : in) {
				optimizationPreventer += statistics.getTotalSubjectFrequency(statement.getSubjectAsLong());
				optimizationPreventer += statistics.getTotalPropertyFrequency(statement.getPropertyAsLong());
				optimizationPreventer += statistics.getTotalObjectFrequency(statement.getObjectAsLong());
			}
			in.close();
		}
		long time = System.currentTimeMillis() - start;
		System.out.println("Reading took " + (time / 1000) + " sec");
		System.out.println(optimizationPreventer);
		if (WRITE_STATISTICS_DATA) {
			// Read statistics and write into csv
			System.out.println("Writing statistics to file...");
			writeStatisticsToCSV(encodedChunksDir, statisticsDB);
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

	private static void writeStatisticsToCSV(File outputDir, GraphStatisticsDatabase statisticsDB) {
		// There is currently no easy way to obtain the maxId on a db that was reopened
		// This is the max id of the dataset hash_4C_6M
		long maxId = 3625965;
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
