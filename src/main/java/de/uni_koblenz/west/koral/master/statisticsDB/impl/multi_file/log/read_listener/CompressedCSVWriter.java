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
	private Long[] aggregatedBinaries;

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

	public void addRecord(Object... values) {
		addRecordWithBinaries(values, null);
	}

	public void addRecordWithBinaries(Object[] values, byte[] binaryValues) {
		addRecordWithBinaries(rowCounter, values, binaryValues);
		rowCounter++;
	}

	public void addRecordWithBinaries(long recordId, Object[] values, byte[] binaryValues) {
//		System.out.println(recordId);
		if (binaryValues != null) {
			if (aggregatedBinaries == null) {
				aggregatedBinaries = new Long[binaryValues.length];
				Arrays.fill(aggregatedBinaries, 0L);
			}
			for (int i = 0; i < binaryValues.length; i++) {
				aggregatedBinaries[i] += binaryValues[i];
			}
			aggregationCounter++;
		}
		if (!Arrays.equals(currentValues, values)) {
			Object[] aggregatedValues = aggregateBinaries();
			if (currentValues != null) {
				printValue(recordId - 1);
				printValues(currentValues);
				if (aggregatedValues != null) {
					printValues(aggregatedValues);
				}
				println();
			}
			printValue(recordId);
			printValues(values);
			if (aggregatedValues != null) {
				printValues(aggregatedValues);
			}
			println();
			currentValues = values.clone();
		}
	}

	private Object[] aggregateBinaries() {
		Object[] aggregatedValues = null;
		if (aggregatedBinaries != null) {
			// Do the aggregation by dividing by the amount of collected values
			for (int i = 0; i < aggregatedBinaries.length; i++) {
				aggregatedBinaries[i] = Math.round((aggregatedBinaries[i] / (double) aggregationCounter) * 100);
			}
			aggregatedValues = aggregatedBinaries.clone();
			// Reset array for next aggregation interval
			Arrays.fill(aggregatedBinaries, 0L);
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
		printValue(recordId);
		printValues(currentValues);
		Object[] aggregatedValues = aggregateBinaries();
		if (aggregatedValues != null) {
			printValues(aggregatedValues);
		}
		println();
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
