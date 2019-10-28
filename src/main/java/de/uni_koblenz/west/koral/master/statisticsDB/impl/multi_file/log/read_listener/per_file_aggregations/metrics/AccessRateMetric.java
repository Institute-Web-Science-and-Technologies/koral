package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.metrics;

import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogEvent;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.Metric;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.aggregations.GlobalPercentageAggregator;

/**
 * Collects the amount of accesses.
 *
 * @author Philipp Töws
 *
 */
public class AccessRateMetric extends Metric {

	public AccessRateMetric() {
		aggregator = new GlobalPercentageAggregator();
	}

	@Override
	public String getName() {
		return "ACCESS_RATE";
	}

	@Override
	public void accumulate(int rowType, Map<String, Object> data) {
		if (rowType == StorageLogEvent.READWRITE.ordinal()) {
			aggregator.accumulate(1);
		}
	}

}
