package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.metrics;

import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.Metric;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.aggregations.GlobalPercentageAggregator;

public class AccessRateMetric extends Metric {

	public AccessRateMetric() {
		aggregator = new GlobalPercentageAggregator();
	}

	@Override
	public String getName() {
		return "ACCESS_RATE";
	}

	@Override
	public void accumulate(Map<String, Object> data) {
		aggregator.accumulate(1);
	}

}
