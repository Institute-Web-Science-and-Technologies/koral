package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.metrics;

import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.Metric;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.aggregations.FileLocalAverageAggregator;

public class CacheUsageMetric extends Metric {

	public CacheUsageMetric() {
		aggregator = new FileLocalAverageAggregator();
	}

	@Override
	public String getName() {
		return "CACHE_USAGE";
	}

	@Override
	public void accumulate(Map<String, Object> data) {
		aggregator.accumulate((int) data.get(StorageLogWriter.KEY_ACCESS_CACHEUSAGE));
	}

}
