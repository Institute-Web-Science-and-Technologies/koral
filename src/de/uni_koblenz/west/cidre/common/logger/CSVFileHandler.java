package de.uni_koblenz.west.cidre.common.logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;

/**
 * Writes log messages to a local CSV file.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class CSVFileHandler extends Handler {

	private final BufferedWriter out;

	private final Formatter formatter;

	private String delim = "";

	public CSVFileHandler(Configuration conf, String[] currentServer)
			throws IOException {
		out = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(conf.getLogDirectory() + File.separatorChar
						+ currentServer[0] + "_" + currentServer[1] + ".log"),
				"UTF-8"));
		formatter = new CSVFormatter(currentServer);
		out.write(formatter.getHead(this));
		delim = "\n";
	}

	@Override
	public void publish(LogRecord record) {
		try {
			out.write(delim);
			out.write(formatter.format(record));
			delim = "\n";
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void flush() {
		try {
			out.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() throws SecurityException {
		try {
			out.write(delim);
			out.write(formatter.getTail(this));
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
