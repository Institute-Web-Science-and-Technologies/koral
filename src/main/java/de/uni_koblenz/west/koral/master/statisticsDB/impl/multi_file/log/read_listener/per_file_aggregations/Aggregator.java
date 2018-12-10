package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations;

import java.util.Arrays;

/**
 * Collects/Accumulates multiple different data values (by addition) and provides a single representative value for each
 * data column on aggregation. The number of columns has to be predetermined in the constructor. Accumulation entries do
 * not have to have data for each column, individual accumulation counters are provided. How the accumulated sum is
 * aggregated must be specified in the abstract method {@link #aggregate(long, long, long)}.
 *
 * @author Philipp Töws
 *
 */
public abstract class Aggregator {

	protected final long[] accumulations;

	protected final long[] accumulationCounter;

	private final int valueCount;

	public Aggregator(int valueCount) {
		this.valueCount = valueCount;
		accumulationCounter = new long[valueCount];
		Arrays.fill(accumulationCounter, 0L);
		accumulations = new long[valueCount];
	}

	/**
	 * Add data values for all columns. Empty or missing values can be represented as null.
	 * 
	 * @param values
	 *            A list of values, one for each column. Must have the length that was given to the constructor.
	 */
	public void accumulate(Number... values) {
		if (values.length != valueCount) {
			throw new IllegalArgumentException(
					"Values argument must have the length that was given on construction. Missing values can be represented as nulls.");
		}
		for (int i = 0; i < values.length; i++) {
			if (values[i] != null) {
				accumulations[i] += values[i].longValue();
				accumulationCounter[i]++;
			}
		}
	}

	/**
	 * Aggregates the values that were accumulated by {@link #accumulate(Number...)}. The aggregation method must be
	 * specified by overriding {@link #aggregate(long, long)}. Internally, {@link #aggregate(long)} is called with an
	 * extraValue of 0.
	 *
	 * @return An array of the same length as the accumulation array, containg the aggregated values. If no values were
	 *         accumulated since the last aggregation, the array contains only zeroes.
	 */
	public Float[] aggregate() {
		return aggregate(0);
	}

	/**
	 * Aggregates the values that were accumulated by {@link #accumulate(Number...)}. The aggregation method must be
	 * specified by overriding {@link #aggregate(long, long)}.
	 *
	 * @param extraValue
	 *            Can be provided to be an available value for the overridden {@link #aggregate(long, long)}. If this is
	 *            not necessary, use {@link #aggregate()}. This value might be used to aggregate the values from a
	 *            global perspective, by using e.g. a global value counter instead of one for this series only.
	 * @return An array of the same length as the accumulation array, containg the aggregated value. For each metric
	 *         that didn't accumulate any values, the array contains a null.
	 */
	public Float[] aggregate(long extraValue) {
		Float[] aggregations = new Float[valueCount];
		for (int i = 0; i < accumulations.length; i++) {
			// Only aggregate metrics where values were accumulated
			if (accumulationCounter[i] > 0) {
				aggregations[i] = aggregate(accumulations[i], accumulationCounter[i], extraValue);
			}
		}
		return aggregations;
	}

	protected abstract float aggregate(long accumulatedValue, long accumulationCounter, long extraValue);

	public void reset() {
		Arrays.fill(accumulationCounter, 0L);
		Arrays.fill(accumulations, 0L);
	}

}
