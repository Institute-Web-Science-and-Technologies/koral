package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.metrics;

import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogEvent;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.Metric;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.aggregations.FileLocalPercentageAggregator;

/**
 * Collects the amount of write accesses.
 * 
 * @author Philipp Töws
 *
 */
public class WriteRateMetric extends Metric {

	public WriteRateMetric() {
		aggregator = new FileLocalPercentageAggregator();
	}

	@Override
	public String getName() {
		return "WRITE_RATE";
	}

	@Override
	public void accumulate(int rowType, Map<String, Object> data) {
		if (rowType == StorageLogEvent.READWRITE.ordinal()) {
			aggregator.accumulate((byte) data.get(StorageLogWriter.KEY_ACCESS_WRITE));
		}
	}

}
