package playground;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatisticsDatabase;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.MultiFileGraphStatisticsDatabase;

/**
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class StatisticsDBTest {

	public static void main(String[] args) throws FileNotFoundException {

		if (args.length != 1) {
			System.out.println("Usage: java " + StatisticsDBTest.class.getName() + " <encodedChunksDir>");
		}
		File encodedChunksDir = new File(args[0]);
		if (!encodedChunksDir.exists() || !encodedChunksDir.isDirectory()) {
			System.err.println("Directory does not exist: " + encodedChunksDir);
		}
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
		short numberOfChunks = 4;
		Configuration conf = new Configuration();

		System.out.println("Collecting Statistics...");

		File statisticsDir = new File(conf.getStatisticsDir(true));
		if (statisticsDir.exists()) {
			try {
				FileUtils.cleanDirectory(statisticsDir);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		GraphStatisticsDatabase statisticsDB = new MultiFileGraphStatisticsDatabase(conf.getStatisticsDir(true),
				numberOfChunks);
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
			// Read statistics and write into csv
			System.out.println("Writing statistics to file...");
			try {
				CSVFormat csvFileFormat = CSVFormat.RFC4180.withRecordSeparator('\n');
				CSVPrinter printer = new CSVPrinter(
						new OutputStreamWriter(new FileOutputStream(encodedChunksDir.getCanonicalPath() + File.separator
								+ statisticsDB.getClass().getSimpleName() + "-statistics.csv"), "UTF-8"),
						csvFileFormat);
				for (long l : statisticsDB.getChunkSizes()) {
					printer.print(l);
				}
				printer.println();
				for (int id = 1; id < statisticsDB.getMaxId(); id++) {
					for (long l : statisticsDB.getStatisticsForResource(id)) {
						printer.print(l);
					}
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
		System.out.println("Finished.");

	}

}
