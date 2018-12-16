package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogEvent;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogReadListener;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.metrics.AccessRateMetric;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.metrics.CacheHitsMetric;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.metrics.TimeMetric;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.metrics.TimeMetric.TimeMetricType;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.metrics.WriteRateMetric;

/**
 * Acts as an adapter between the StorageLogEvent calls and the individual aggregation instances for each file. Creates
 * and manages these instances per file while also triggering a simultaneous aggregation for all files. Also determiens
 *
 * @author Philipp Töws
 *
 */
public class SampledAggregationsListener implements StorageLogReadListener {

	private final Map<Byte, FileAggregator> files;

	private final long samplingInterval;

	private long globalRowCounter;

	private long globalAccumulationCounter;

	private final boolean alignToGlobal;

	private final String outputPath;

	public SampledAggregationsListener(long samplingInterval, boolean alignToGlobal, String outputPath) {
		this.samplingInterval = samplingInterval;
		this.alignToGlobal = alignToGlobal;
		this.outputPath = outputPath;

		files = new HashMap<>();
		globalRowCounter = 0;
	}

	@Override
	public void onLogRowRead(int rowType, Map<String, Object> data) {
		if (rowType == StorageLogEvent.READWRITE.ordinal()) {
			byte fileId = (byte) data.get(StorageLogWriter.KEY_FILEID);
			if (!files.containsKey(fileId)) {
				String fileName = "aggregations_fileId" + fileId + (alignToGlobal ? "_globalAligned" : "") + ".csv.gz";
				File file = new File(outputPath, fileName);
				// TODO: Does it make sense to collect all metrics both from a local and global perspective?
				LinkedList<Metric> metrics = new LinkedList<>();
				metrics.add(new AccessRateMetric());
				metrics.add(new CacheHitsMetric());
				for (TimeMetricType type : TimeMetricType.values()) {
					metrics.add(new TimeMetric(type));
				}
				metrics.add(new WriteRateMetric());
				if (!alignToGlobal) {
					files.put(fileId, new FileAggregator(metrics, file, alignToGlobal));
				} else {
					files.put(fileId, new FileAggregator(metrics, file, alignToGlobal, globalRowCounter));
				}
			}
			files.get(fileId).accumulate(data);
			globalAccumulationCounter++;
			globalRowCounter++;
			// TODO: This always aggregates windows determined by the global event count. Maybe for !alignToGlobal each
			// file should aggregate when its own counter reached a certain limit?
			if (((globalRowCounter % samplingInterval) == 0) && (globalRowCounter > 0)) {
				for (FileAggregator file : files.values()) {
					file.printAggregations(globalRowCounter, globalAccumulationCounter);
				}
				globalAccumulationCounter = 0;
			}
		}

	}

	@Override
	public void close() {
		for (FileAggregator file : files.values()) {
			file.printAggregations(globalRowCounter, globalAccumulationCounter);
			file.close();
		}
	}

}
