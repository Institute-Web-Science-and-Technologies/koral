package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.metrics;

import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogEvent;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.Metric;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.aggregations.FileLocalAverageAggregator;

/**
 * Collects the cache usage values.
 *
 * @author Philipp Töws
 *
 */
public class CacheUsageMetric extends Metric {

	public CacheUsageMetric() {
		aggregator = new FileLocalAverageAggregator();
	}

	@Override
	public String getName() {
		return "CACHE_USAGE";
	}

	@Override
	public void accumulate(int rowType, Map<String, Object> data) {
		if ((rowType == StorageLogEvent.READWRITE.ordinal()) || (rowType == StorageLogEvent.BLOCKFLUSH.ordinal())) {
			aggregator.accumulate((int) data.get(StorageLogWriter.KEY_CACHEUSAGE));
		}
	}

}
