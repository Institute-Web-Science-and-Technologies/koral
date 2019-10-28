package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.aggregations;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.Aggregator;

/**
 * Aggregates an accumulated value as a ratio to the amount of accumulated values.
 *
 * @author Philipp Töws
 *
 */
public class FileLocalPercentageAggregator extends Aggregator {

	public FileLocalPercentageAggregator() {}

	@Override
	protected float aggregate(long accumulatedValue, long accumulationCounter, long extraValue) {
		return (accumulatedValue / (float) accumulationCounter) * 100;
	}

}
