package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.aggregations;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.Aggregator;

/**
 * Aggregates an accumulated value as a ratio to the global amount of rows.
 *
 * @author Philipp Töws
 *
 */
public class GlobalPercentageAggregator extends Aggregator {

	public GlobalPercentageAggregator() {}

	@Override
	protected float aggregate(long accumulatedValue, long accumulationCounter, long extraValue) {
		// extraValue is supposed to be globalRowCounter
		return (accumulatedValue / (float) extraValue) * 100;
	}

}
