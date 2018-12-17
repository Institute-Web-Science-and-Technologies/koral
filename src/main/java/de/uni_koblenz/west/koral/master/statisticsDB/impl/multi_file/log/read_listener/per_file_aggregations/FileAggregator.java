package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.CSVWriter;

/**
 * Wraps
 * {@link de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener.per_file_aggregations.Aggregator}
 * by aggregating data for an individual file, given metrics and a given aligning perspective (local or global).
 *
 * @author Philipp Töws
 *
 */
public class FileAggregator {

	private long rowCounter;

	private final CSVWriter csvWriter;

	private final List<Metric> metrics;

	private final boolean alignToGlobal;

	/**
	 *
	 * @param metrics
	 *            List of the metrics that this file will accumualte and aggregate. The reference is internally used as
	 *            given, no copy is made.
	 * @param outputFile
	 *            The output file the result CSV is written to.
	 * @param alignToGlobal
	 *            Whether the data is collected from a global (=true) or local (=false) perspective is collected.
	 * @param firstRecordId
	 *            The id for the first record. Might be non-zero if this Aggregator is instantiated later in a reading
	 *            progress and alignToGlobal is true, so that the data starts at the correct global x-Value.
	 */
	public FileAggregator(List<Metric> metrics, File outputFile, boolean alignToGlobal, long firstRecordId) {
		this.metrics = metrics;
		this.alignToGlobal = alignToGlobal;
		csvWriter = new CSVWriter(outputFile, firstRecordId);
		rowCounter = 0;

		// Collect column headers and write into csv
		LinkedList<String> headers = new LinkedList<>();
		for (Metric metric : metrics) {
			headers.add(metric.getName());
		}
		csvWriter.printHeader(headers.toArray());
	}

	/**
	 * Calls {@link #FileAggregator(List, File, boolean, long)} with a firstRecordId of zero.
	 *
	 * @param metrics
	 * @param outputFile
	 * @param alignToGlobal
	 */
	public FileAggregator(List<Metric> metrics, File outputFile, boolean alignToGlobal) {
		this(metrics, outputFile, alignToGlobal, 0);
	}

	/**
	 * Extracts the data for the metrics from the event data and accumulates them in their corresponding aggregator.
	 *
	 * @param data
	 *            The event data that maps from data identifiers (as defined in StorageLogWriter) to their values.
	 */
	public void accumulate(int rowType, Map<String, Object> data) {
		for (Metric metric : metrics) {
			metric.accumulate(rowType, data);
		}
		rowCounter++;
	}

	/**
	 * Triggers aggregation for each aggregator and writes the results into the output csv file. Aggregation results are
	 * rounded to two decimal places before written into the file to prevent ugly floating point representation errors.
	 *
	 * @param globalRowCounter
	 *            The amount of all events that have been collected until now.
	 * @param globalAccumulationCounter
	 *            The amount of events that have been collected since the last aggregation.
	 */
	public void printAggregations(long globalRowCounter, long globalAccumulationCounter) {
		Float[] aggregations = aggregate(globalAccumulationCounter);
		for (int i = 0; i < aggregations.length; i++) {
			if (aggregations[i] != null) {
				// Round to two decimal places
				aggregations[i] = Math.round(aggregations[i] * 100) / (float) 100;
			}
		}
		if (alignToGlobal) {
			csvWriter.addIntervalRecord(globalRowCounter - 1, (Object[]) aggregations);
		} else {
			csvWriter.addIntervalRecord(rowCounter - 1, (Object[]) aggregations);
		}
	}

	/**
	 * Aggregate all metrics.
	 *
	 * @param globalAccumulationCounter
	 * @return Array of aggregation results, values in order of {@link #metrics}. Values of null indicate no
	 *         accumulations since last aggregation.
	 */
	private Float[] aggregate(long globalAccumulationCounter) {
		Float[] allAggregations = new Float[metrics.size()];
		// Use separate indexing variable so the iterating can make use of LinkedLists O(1) next-calls
		int i = 0;
		for (Metric metric : metrics) {
			allAggregations[i] = metric.getAggregator().aggregate(globalAccumulationCounter);
			metric.getAggregator().reset();
			i++;
		}
		return allAggregations;
	}

	public void close() {
		csvWriter.close();
	}

}
