package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/**
 * Simple class to write data into a gzipped CSV file. Supports plain and interval records, i.e. if interval records are
 * given, they are duplicated and written into the file with the first id of the interval and the last.
 *
 * @author Philipp Töws
 *
 */
public class CSVWriter {

	private final CSVPrinter csvPrinter;

	private long rowCounter;

	private long lastId;

	/**
	 *
	 * @param csvFile
	 * @param firstRecordId
	 *            The ID were the record ids will start at. For interval records, this value should be set to the ID
	 *            where the recorded object was observed for the first time (i.e. the first global row id where the file
	 *            this writer is responsible for had its first log event).
	 */
	public CSVWriter(File csvFile, long firstRecordId) {
		CSVFormat csvFileFormat = CSVFormat.RFC4180.withRecordSeparator('\n');
		try {
			csvPrinter = new CSVPrinter(
					new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(csvFile, false)), "UTF-8"),
					csvFileFormat);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		rowCounter = 0;
		// Subtract 1 because 1 is added in addIntervalRecord to get the left interval id
		lastId = firstRecordId - 1;
	}

	public CSVWriter(File csvFile) {
		this(csvFile, 0);
	}

	/**
	 * Prints the header of the CSV file, i.e. the labels for each column. A label for the row/record id column is
	 * always prepended.
	 *
	 * @param values
	 */
	public void printHeader(Object... values) {
		printValue("ROW");
		printValues(values);
		println();
	}

	/**
	 * Add a record to which an internal record id is assigned. On each call, the internal rowCounter is incremented.
	 *
	 * @param values
	 */
	public void addRecord(Object... values) {
		addRecord(rowCounter, values);
		rowCounter++;
	}

	/**
	 * Add a record with the given custom record id.
	 *
	 * @param recordId
	 *            Custom id that is prepended to the record. Can be used to align to a different x axis
	 * @param values
	 *            The plain values that will be included. Changes in this array cause a new record with all contents
	 */
	public void addRecord(long recordId, Object[] values) {
		printRecordWithId(recordId, values);
		lastId = recordId;
	}

	/**
	 * Add a record that describes the interval from the last added record id to the given record id. Two records will
	 * be printed, one with the last added record id plus one as id, and one with the given record id. The rest of these
	 * two records represent the given values.
	 *
	 * @param recordId
	 *            The last record id of the recorded interval.
	 * @param values
	 *            The values that describe this interval.
	 */
	public void addIntervalRecord(long recordId, Object... values) {
		printRecordWithId(lastId + 1, values);
		printRecordWithId(recordId, values);
		lastId = recordId;
	}

	private void printRecordWithId(long recordId, Object[] values) {
		printValue(recordId);
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

	public void close() {
		try {
			csvPrinter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
