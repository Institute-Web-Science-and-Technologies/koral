package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.read_listener;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public class CompressedCSVWriter {

	private final CSVPrinter csvPrinter;

	public CompressedCSVWriter(File csvFile) {
		CSVFormat csvFileFormat = CSVFormat.RFC4180.withRecordSeparator('\n');
		try {
			csvPrinter = new CSVPrinter(
					new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(csvFile, false)), "UTF-8"),
					csvFileFormat);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void print(Object... values) {
		try {
			csvPrinter.printRecord(values);
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
