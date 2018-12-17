package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.metrics;

import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogEvent;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.Metric;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.aggregations.FileLocalAverageAggregator;

/**
 * Extracts the time used for each event. Each instance represents only one of four different case distinctions that
 * correspond to different access types, namely file or cache access, and read or write operation. Additionally, a case
 * for all types exist to record all the time. All possible instance/pattern types are listed in {@link TimeMetricType}.
 *
 * @author Philipp Töws
 *
 */
public class TimeMetric extends Metric {

	/**
	 * Since there are multiple ways to aggregate used time as a metric, this enum represents all implemented ones. The
	 * behaviour and states are almost equal to all instances, except the name and different "filters". For example, it
	 * is desired to differentiate between time that is used for file accesses and for cache accesses, because their
	 * order of magnitude vary vastly. Therefore, each instance is supposed to listen only for a certain combination of
	 * access types.
	 *
	 * @author Philipp Töws
	 *
	 */
	public static enum TimeMetricType {
	CACHE_READ("TIME_AVG_CACHE_READ", Trinary.TRUE, Trinary.FALSE),
	CACHE_WRITE("TIME_AVG_CACHE_WRITE", Trinary.TRUE, Trinary.TRUE),
	FILE_READ("TIME_AVG_FILE_READ", Trinary.FALSE, Trinary.FALSE),
	FILE_WRITE("TIME_AVG_FILE_WRITE", Trinary.FALSE, Trinary.TRUE),
	TOTAL("TIME_AVG_TOTAL", Trinary.ANY, Trinary.ANY);

		/*
		 * Provides a simple and reduced ternary (three-valued) logic. Additionally to true and false there exists a
		 * state "any", which, compared to both other values, returns always true.
		 *
		 * This general approach allows simple handling of the total time metric, while also facilitating different
		 * combinations in the future, e.g. an instance that listens to all file access times.
		 */
		public static enum Trinary {
			TRUE,
			FALSE,
			ANY;

			/**
			 *
			 * @param trinary
			 *            An instance of the trinary logic.
			 * @param bool
			 *            An instance of the boolean logic.
			 * @return A boolean according to the trinary logic. If the trinary is TRUE or FALSE, the result is the same
			 *         as from the boolean logic, if it is ANY, the return value is always true.
			 */
			public static boolean equals(Trinary trinary, boolean bool) {
				switch (trinary) {
				case TRUE:
					return bool;
				case FALSE:
					return !bool;
				case ANY:
					return true;
				default:
					throw new IllegalArgumentException("Unknown trinary: " + trinary);
				}
			}
		}

		private final String name;
		/**
		 * Flag that shows whether this type listens to cache accesses (=true) or file accesses (=false).
		 */
		private final Trinary listensForCacheAccesses;
		/**
		 * Flag that shows whether this type listens to write accesses (=true) or read accesses (=false).
		 */
		private final Trinary listensForWriteAccesses;

		TimeMetricType(String name, Trinary listensForCacheAccesses, Trinary listensForWriteAccesses) {
			this.name = name;
			this.listensForCacheAccesses = listensForCacheAccesses;
			this.listensForWriteAccesses = listensForWriteAccesses;
		}

		public String getName() {
			return name;
		}

		public boolean listensFor(boolean cacheAccess, boolean writeAccess) {
			return Trinary.equals(listensForCacheAccesses, cacheAccess)
					&& Trinary.equals(listensForWriteAccesses, writeAccess);
		}
	}

	private final TimeMetricType type;

	public TimeMetric(TimeMetricType type) {
		this.type = type;

		aggregator = new FileLocalAverageAggregator();
	}

	@Override
	public String getName() {
		return type.getName();
	}

	@Override
	public void accumulate(int rowType, Map<String, Object> data) {
		if (rowType == StorageLogEvent.READWRITE.ordinal()) {
			boolean cacheAccess = (byte) data.get(StorageLogWriter.KEY_ACCESS_CACHEHIT) > 0;
			boolean write = (byte) data.get(StorageLogWriter.KEY_ACCESS_WRITE) > 0;
			if (type.listensFor(cacheAccess, write)) {
				aggregator.accumulate((long) data.get(StorageLogWriter.KEY_ACCESS_TIME));
			}
		}
	}

}
