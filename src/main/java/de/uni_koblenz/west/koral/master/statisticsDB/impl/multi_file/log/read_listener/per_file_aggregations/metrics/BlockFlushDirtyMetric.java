package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.metrics;

import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.Metric;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.aggregations.FileLocalAverageAggregator;

public class BlockFlushDirtyMetric extends Metric {

	public BlockFlushDirtyMetric() {
		aggregator = new FileLocalAverageAggregator();
	}

	@Override
	public String getName() {
		return "PERCENTAGE_DIRTY";
	}

	@Override
	public void accumulate(Map<String, Object> data) {
		aggregator.accumulate((byte) data.get(StorageLogWriter.KEY_BLOCKFLUSH_DIRTY));
	}

}
