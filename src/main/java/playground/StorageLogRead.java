package playground;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.CompressedLogReader;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogReadListener;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.FileListenerManager;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.ImplementationListener;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.ReadProgressionListener;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.SampledAggregationsListener;

public class StorageLogRead {

	public static void main(String[] args) {
		String outputPath = "/home/philipp/Development/koral_benchmark/statistics_benchmark/storageLogAnalysis";
		File storageFile = new File("/tmp/master/statistics/storageLog.gz");

		long samplingInterval = 100_000;

		long start = System.currentTimeMillis();

		CompressedLogReader logReader = new CompressedLogReader(storageFile);
		ReadProgressionListener progressionListener = new ReadProgressionListener(10_000_000);

		List<StorageLogReadListener> listeners = new LinkedList<>();

		listeners.add(new FileListenerManager(true, outputPath));
		listeners.add(new FileListenerManager(false, outputPath));
		listeners.add(new SampledAggregationsListener(samplingInterval, true, outputPath));
		listeners.add(new SampledAggregationsListener(samplingInterval, false, outputPath));
		// Not necessary because the total cache usage is calculated by the stacked area cache usage plot
//		listeners.add(new ExtraCacheUsageListener(outputPath));
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
					+ String.format("%,d", entry.getValue()));
		}
		System.out
				.println("Read a total of " + String.format("%,d", progressionListener.getCurrentRowCount()) + " rows");
		System.out.println("Finished in " + StatisticsDBTest.formatTime(time));
	}

}
