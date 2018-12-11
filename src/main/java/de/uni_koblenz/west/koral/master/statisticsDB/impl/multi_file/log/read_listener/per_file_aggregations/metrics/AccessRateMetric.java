package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.metrics;

import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.FileAggregator.AggregationMethod;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.Metric;

public class AccessRateMetric implements Metric {

	public AccessRateMetric() {}

	@Override
	public String getName() {
		return "ACCESS_RATE";
	}

	@Override
	public AggregationMethod getAggregatorType() {
		return AggregationMethod.GLOBAL_PERCENTAGE;
	}

	@Override
	public long accumulate(Map<String, Object> data) {
		return 0;
	}

}
