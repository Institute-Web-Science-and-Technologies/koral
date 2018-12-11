package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations;

import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.FileAggregator.AggregationMethod;

public interface Metric {

	/**
	 * @return The name that is used as CSV column header.
	 */
	public String getName();

	/**
	 * How the accumulated data is supposed to be aggregated, e.g. as percentage or average. Possible values are listed
	 * in enum {@link AggregationMethod}.
	 *
	 * @return
	 */
	public AggregationMethod getAggregatorType();

	/**
	 * Extract the value of interest from the data of an event.
	 *
	 * @param data
	 *            The data of a StorageLog event.
	 * @return The value that will be added onto the accumulation value.
	 */
	public long accumulate(Map<String, Object> data);
}
