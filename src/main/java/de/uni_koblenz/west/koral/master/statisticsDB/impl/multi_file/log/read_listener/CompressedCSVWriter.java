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

	private long rowCounter;

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
	}

	public void printHeader(Object... values) {
		printRecord(values);
	}

	public void addRecord(Object... values) {
		if (!Arrays.equals(currentValues, values)) {
			if (currentValues != null) {
				printValue(rowCounter - 1);
				printRecord(currentValues);
			}
			printValue(rowCounter);
			printRecord(values);
			currentValues = values;
		}
		rowCounter++;
	}

	private void printRecord(Object... values) {
		// The csvPrinter.printRecord() method is not used, because it doesn't prepend value separators in case single
		// values were already printed for this record
		for (Object value : values) {
			printValue(value);
		}
		try {
			csvPrinter.println();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void printValue(Object value) {
		try {
			csvPrinter.print(value);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void close() {
		try {
			csvPrinter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
