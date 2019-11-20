package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.metrics;

import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogEvent;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.Metric;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.aggregations.FileLocalAverageAggregator;

/**
 * Collects the percentage of how much of the total data is cached.
 *
 * @author Philipp Töws
 *
 */
public class PercentageCachedMetric extends Metric {

	public PercentageCachedMetric() {
		aggregator = new FileLocalAverageAggregator();
	}

	@Override
	public String getName() {
		return "PERCENTAGE_CACHED";
	}

	@Override
	public void accumulate(int rowType, Map<String, Object> data) {
		if ((rowType == StorageLogEvent.READWRITE.ordinal()) || (rowType == StorageLogEvent.BLOCKFLUSH.ordinal())) {
			aggregator.accumulate((byte) data.get(StorageLogWriter.KEY_PERCENTAGECACHED));
		}
	}

}
