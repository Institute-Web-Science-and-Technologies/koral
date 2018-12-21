package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations;

import java.util.Map;

public abstract class Metric {

	protected Aggregator aggregator;

	/**
	 * @return The name that is used as CSV column header.
	 */
	public abstract String getName();

	/**
	 * @return An Aggregator instance that can aggregate the metric data in a way that makes sense for this metric. For
	 *         example, some metrics like share of all accesses don't make sense if aggregated with a local perspective.
	 */
	public Aggregator getAggregator() {
		return aggregator;
	}

	/**
	 * Accumulate the value of interest from the data of an event. The extracted value from the data map is given to the
	 * internal Aggregator.
	 *
	 * @param data
	 *            The data of a StorageLog event.
	 * @return The value that will be added onto the accumulation value.
	 */
	public abstract void accumulate(Map<String, Object> data);
}
