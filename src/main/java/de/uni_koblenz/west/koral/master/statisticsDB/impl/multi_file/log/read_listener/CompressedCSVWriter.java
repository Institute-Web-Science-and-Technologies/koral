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

	private long aggregationCounter;

	private boolean finished;

	public CompressedCSVWriter(File csvFile) {
		CSVFormat csvFileFormat = CSVFormat.RFC4180.withRecordSeparator('\n');
		try {
			csvPrinter = new CSVPrinter(
					new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(csvFile, false)), "UTF-8"),
					csvFileFormat);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		rowCounter = 0;
		finished = false;
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
		rowCounter++;
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
		if (!Arrays.equals(currentValues, values)) {
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
			aggregationCounter++;
		}
		if (accNumbers != null) {
			accumulatedNumbers = accumulateValues(accumulatedNumbers, accNumbers);
		}
	}

	/**
	 * Finishes the two records of the previous interval with the data that was aggregated over this interval
	 *
	 * @param recordId
	 *            The record id of the right record of the previous interval
	 */
	private void finishPreviousInterval(long recordId) {
		Object[] aggregatedBinaries = aggregateBinaries();
		if (aggregatedBinaries != null) {
			printValues(aggregatedBinaries);
		}
		if (accumulatedNumbers != null) {
			printValues((Object[]) accumulatedNumbers);
		}
		println();
		// Print right interval record for previous interval
		printValue(recordId - 1);
		printValues(currentValues);
		if (aggregatedBinaries != null) {
			printValues(aggregatedBinaries);
		}
		if (accumulatedNumbers != null) {
			printValues((Object[]) accumulatedNumbers);
			Arrays.fill(accumulatedNumbers, 0L);
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

	private Object[] aggregateBinaries() {
		Object[] aggregatedValues = null;
		if (accumulatedBinaries != null) {
			// Do the aggregation by dividing by the amount of collected values
			for (int i = 0; i < accumulatedBinaries.length; i++) {
				accumulatedBinaries[i] = Math.round((accumulatedBinaries[i] / (double) aggregationCounter) * 100);
			}
			aggregatedValues = accumulatedBinaries.clone();
			// Reset array for next aggregation interval
			Arrays.fill(accumulatedBinaries, 0L);
			aggregationCounter = 0;
		}
		return aggregatedValues;
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
