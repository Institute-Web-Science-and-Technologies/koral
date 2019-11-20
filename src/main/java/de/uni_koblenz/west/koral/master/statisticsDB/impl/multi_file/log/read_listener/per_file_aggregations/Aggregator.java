package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations;

/**
 * Collects/accumulates a series of data values (by addition) and provides a single representative value on aggregation.
 * How the accumulated sum is aggregated must be specified in the abstract method {@link #aggregate(long, long, long)}.
 *
 * @author Philipp Töws
 *
 */
public abstract class Aggregator {

	protected long accumulation;

	protected long accumulationCounter;

	public Aggregator() {
		reset();
	}

	/**
	 * Add a data value.
	 *
	 * @param value
	 *            A new value in the data series to be accumulated.
	 */
	public void accumulate(Number value) {
		accumulation += value.longValue();
		accumulationCounter++;
	}

	/**
	 * Aggregates the values that were accumulated by {@link #accumulate(long)}. The aggregation method must be
	 * specified by overriding {@link #aggregate(long, long)}. Internally, {@link #aggregate(long)} is called with an
	 * extraValue of 0.
	 *
	 * @return The result of the aggregation, null if no values where accumualted since the last aggregation.
	 */
	public Float aggregate() {
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
	 * @return The result of the aggregation, null if no values where accumualted since the last aggregation.
	 */
	public Float aggregate(long extraValue) {
		// Only aggregate if there were values accumulated
		if (accumulationCounter > 0) {
			return aggregate(accumulation, accumulationCounter, extraValue);
		}
		return null;
	}

	protected abstract float aggregate(long accumulatedValue, long accumulationCounter, long extraValue);

	public void reset() {
		accumulation = 0;
		accumulationCounter = 0;
	}

}
