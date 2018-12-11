package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.metrics;

import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.FileAggregator.AggregationMethod;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.Metric;

public class WriteRateMetric implements Metric {

	public WriteRateMetric() {}

	@Override
	public String getName() {
		return "WRITE_RATE";
	}

	@Override
	public AggregationMethod getAggregatorType() {
		return AggregationMethod.FILE_LOCAL_PERCENTAGE;
	}

	@Override
	public long accumulate(Map<String, Object> data) {
		return (long) data.get(StorageLogWriter.KEY_ACCESS_WRITE);
	}

}
