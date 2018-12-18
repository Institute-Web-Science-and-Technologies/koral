package playground;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.CompressedLogReader;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogReadListener;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.ChunkSwitchListener;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.ImplementationListener;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.ReadProgressionListener;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.SampledAggregationsListener;

public class StorageLogRead {

	private static void printUsage() {
		System.out.println(
				"Usage: java -jar StorageLogReader.jar <storage file> <output dir> <sampling interval> <maxOpenFilesPerFileId>");
	}

	public static void main(String[] args) {
		if (args.length != 4) {
			System.err.println("Invalid amount of arguments.");
			printUsage();
			return;
		}
		File storageFile = new File(args[0]);
		if (!storageFile.exists() || !storageFile.isFile()) {
			System.err.println("Invalid storage file: " + storageFile);
			printUsage();
			return;
		}
		String outputPath = args[1];
		File outputDir = new File(outputPath);
		if (!outputDir.exists() || !outputDir.isDirectory()) {
			outputDir.mkdirs();
		}
		long samplingInterval;
		try {
			samplingInterval = Long.parseLong(args[2]);
			System.out.println("Parameter Sampling Interval: " + samplingInterval);
		} catch (NumberFormatException e) {
			printUsage();
			System.err.println("Invalid argument for sampling interval: " + args[2]);
			return;
		}
		int maxOpenFilesPerFileId;
		try {
			maxOpenFilesPerFileId = Integer.parseInt(args[3]);
			System.out.println("Parameter maxOpenFilesPerFileId: " + maxOpenFilesPerFileId);
		} catch (NumberFormatException e) {
			printUsage();
			System.err.println("Invalid argument for maxOpenFilesPerFileId: " + args[3]);
			return;
		}

		long start = System.currentTimeMillis();

		CompressedLogReader logReader = new CompressedLogReader(storageFile);
		ReadProgressionListener progressionListener = new ReadProgressionListener(100 * samplingInterval);

		List<StorageLogReadListener> listeners = new LinkedList<>();

		listeners.add(new SampledAggregationsListener(samplingInterval, true, outputPath));
		listeners.add(new SampledAggregationsListener(samplingInterval, false, outputPath));
		listeners.add(new ChunkSwitchListener(outputPath));
		listeners.add(new ImplementationListener(outputPath));
		listeners.add(progressionListener);

		for (StorageLogReadListener listener : listeners) {
			logReader.registerListener(listener);
		}

		logReader.readAll();

		logReader.close();
		for (StorageLogReadListener listener : listeners) {
			listener.close();
		}
		long time = System.currentTimeMillis() - start;
		System.out.println("Listener Computation Times:");
		for (Entry<StorageLogReadListener, Long> entry : logReader.getListenerComputationTimes().entrySet()) {
			System.out.println(String.format("%-32s", entry.getKey().getClass().getSimpleName()) + ": "
					+ StatisticsDBTest.formatTime(entry.getValue() / 1_000_000));
		}
		System.out
				.println("Read a total of " + String.format("%,d", progressionListener.getCurrentRowCount()) + " rows");
		System.out.println("Finished in " + StatisticsDBTest.formatTime(time));
	}

}
