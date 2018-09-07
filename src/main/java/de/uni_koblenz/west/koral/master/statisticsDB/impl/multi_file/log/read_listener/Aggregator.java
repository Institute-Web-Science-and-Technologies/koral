package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener;

import java.util.Arrays;

public abstract class Aggregator {

	protected long[] accumulations;

	protected long accumulationCounter;

	private final int valueCount;

	public Aggregator(int valueCount) {
		this.valueCount = valueCount;
		accumulationCounter = 0;
		accumulations = new long[valueCount];
	}

	public void accumulate(Number... values) {
		if (values.length != valueCount) {
			throw new IllegalArgumentException("Values argument must have the length that was given on construction.");
		}
		for (int i = 0; i < values.length; i++) {
			accumulations[i] += values[i].longValue();
		}
		accumulationCounter++;
	}

	/**
	 * Aggregates the values that were accumulated by {@link #accumulate(Number...)}. The aggregation method must be
	 * specified by overriding {@link #aggregate(long, long)}. Internally, {@link #aggregate(long)} is called with an
	 * extraValue of 0.
	 *
	 * @return An array of the same length as the accumulation array, containg the aggregated values. If no values were
	 *         accumulated since the last aggregation, the array contains only zeroes.
	 */
	public float[] aggregate() {
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
	 * @return An array of the same length as the accumulation array, containg the aggregated value. If no values were
	 *         accumulated since the last aggregation, the array contains only zeroes.
	 */
	public float[] aggregate(long extraValue) {
		float[] aggregations = new float[valueCount];
		if (accumulationCounter == 0) {
			// No data was accumulated for this file
			return aggregations;
		}
		for (int i = 0; i < accumulations.length; i++) {
			aggregations[i] = aggregate(accumulations[i], extraValue);
		}
		return aggregations;
	}

	protected abstract float aggregate(long accumulatedValue, long extraValue);

	public void reset() {
		accumulationCounter = 0;
		Arrays.fill(accumulations, 0L);
	}

}
