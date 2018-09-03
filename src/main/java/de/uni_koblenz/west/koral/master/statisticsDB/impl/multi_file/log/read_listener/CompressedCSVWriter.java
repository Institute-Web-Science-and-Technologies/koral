package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public class CompressedCSVWriter {

	private final CSVPrinter csvPrinter;

	private Object[] currentValues;

	/**
	 * Is of wrapper type to allow polymorphing into Object[] type
	 */
	private Long[] accumulatedBinaries;

	private Long[] accumulatedNumbers;

	private long rowCounter;

	private long accumulationCounterBinaries;

	private boolean finished;

	private long lastAccNumber;

	private final long maxIntervalLength;

	private long lastRecordId;

	public CompressedCSVWriter(File csvFile, long maxIntervalLength) {
		this.maxIntervalLength = maxIntervalLength;
		CSVFormat csvFileFormat = CSVFormat.RFC4180.withRecordSeparator('\n');
		try {
			csvPrinter = new CSVPrinter(
					new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(csvFile, false)), "UTF-8"),
					csvFileFormat);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		rowCounter = 0;
		// Set to -1 to fake a right record id of a previous interval
		lastAccNumber = -1;
		finished = false;
	}

	public CompressedCSVWriter(File csvFile) {
		this(csvFile, Long.MAX_VALUE);
	}

	public void printHeader(Object... values) {
		printValue("ROW");
		printRecord(values);
	}

	public void addSimpleRecord(Object... values) {
		addRecord(values, null, null);
	}

	public void addRecord(Object[] values, Byte[] binaryValues, Long[] accNumbers) {
		addRecord(rowCounter, values, binaryValues, accNumbers);
	}

	/**
	 *
	 * @param recordId
	 *            Custom id that is prepended to the record. Can be used to align to a different x axis
	 * @param values
	 *            The plain values that will be included. Changes in this array cause a new record with all contents
	 * @param binaryValues
	 *            Values that are of binary type and will be aggregated as percentage over each interval
	 * @param accNumbers
	 *            Values that are of numeric type and will be aggregated as a simple sum over each interval
	 */
	public void addRecord(long recordId, Object[] values, Byte[] binaryValues, Long[] accNumbers) {
		if (!Arrays.equals(currentValues, values) || ((recordId - lastRecordId) >= maxIntervalLength)) {
			if (currentValues != null) {
				// At least one record exists
				finishPreviousInterval(recordId - 1);
			}
			// Start printing (right) interval record
			printValue(recordId);
			printValues(values);
			// Clone the parameter because the caller might recycle the array
			currentValues = values.clone();
		}
		if (binaryValues != null) {
			accumulatedBinaries = accumulateValues(accumulatedBinaries, binaryValues);
			accumulationCounterBinaries++;
		}
		if (accNumbers != null) {
			accumulatedNumbers = accumulateValues(accumulatedNumbers, accNumbers);
		}
		lastRecordId = recordId;
		rowCounter++;
	}

	/**
	 * Finishes the two records of the previous interval with the data that was aggregated over this interval
	 *
	 * @param recordId
	 *            The record id of the right record of the previous interval
	 */
	private void finishPreviousInterval(long recordId) {
		Object[] aggregatedBinaries = aggregateValues(accumulatedBinaries, accumulationCounterBinaries);
		if (aggregatedBinaries != null) {
			printValues(aggregatedBinaries);
		}
		Object[] aggregatedNumbers = aggregateValues(accumulatedNumbers, recordId - lastAccNumber);
		if (aggregatedNumbers != null) {
			printValues(aggregatedNumbers);
		}
		println();
		// Print right interval record for previous interval
		printValue(recordId - 1);
		printValues(currentValues);
		if (aggregatedBinaries != null) {
			printValues(aggregatedBinaries);
			resetBinariesAccumulation();
		}
		if (aggregatedNumbers != null) {
			printValues(aggregatedNumbers);
			resetNumbersAccumulation();
			lastAccNumber = recordId;
		}
		println();
	}

	private <T extends Number> Long[] accumulateValues(Long[] accumulationArray, T[] newValues) {
		if (accumulationArray == null) {
			accumulationArray = new Long[newValues.length];
			Arrays.fill(accumulationArray, 0L);
		}
		for (int i = 0; i < newValues.length; i++) {
			accumulationArray[i] += newValues[i].longValue();
		}
		// The array is returned because the argument might have been null which prevents call-by-reference
		// modifications
		return accumulationArray;
	}

	private Object[] aggregateValues(Long[] accumulatedValues, long accumulationCounter) {
		Object[] aggregatedValues = null;
		if (accumulatedValues != null) {
			aggregatedValues = new Object[accumulatedValues.length];
			// Do the aggregation by dividing by the amount of collected values
			for (int i = 0; i < accumulatedValues.length; i++) {
				aggregatedValues[i] = Math.round((accumulatedValues[i] / (double) accumulationCounter) * 100);
			}
		}
		return aggregatedValues;
	}

	private void resetBinariesAccumulation() {
		Arrays.fill(accumulatedBinaries, 0L);
		accumulationCounterBinaries = 0;
	}

	private void resetNumbersAccumulation() {
		Arrays.fill(accumulatedNumbers, 0L);
	}

	private void printRecord(Object... values) {
		// The csvPrinter.printRecord() method is not used, because it doesn't prepend value separators in case single
		// values were already printed for this record
		printValues(values);
		println();
	}

	private void println() {
		try {
			csvPrinter.println();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void printValues(Object... values) {
		for (Object value : values) {
			printValue(value);
		}
	}

	private void printValue(Object value) {
		try {
			csvPrinter.print(value);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void finish(long recordId) {
		// Add last record that wasn't printed yet
//		printValue(recordId);
//		printValues(currentValues);
//		Object[] aggregatedValues = aggregateBinaries();
//		if (aggregatedValues != null) {
//			printValues(aggregatedValues);
//		}
//		println();
		finishPreviousInterval(recordId);
		finished = true;
	}

	public void close() {
		if (!finished) {
			finish(rowCounter);
		}
		try {
			csvPrinter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
