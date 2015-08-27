package de.uni_koblenz.west.cidre.common.logger;

import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import de.uni_koblenz.west.cidre.common.config.Configuration;

public class JeromqStreamHandler extends Handler {

	private final Formatter formatter;

	public JeromqStreamHandler(Configuration conf, String[] currentServer) {
		super();
		formatter = new CSVFormatter(currentServer);
		send(formatter.getHead(this));
		// TODO Auto-generated constructor stub
	}

	@Override
	public void publish(LogRecord record) {
		send(formatter.format(record));
	}

	@Override
	public void flush() {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws SecurityException {
		send(formatter.getTail(this));
		// TODO Auto-generated method stub

	}

	private void send(String message) {
		System.out.println(message);
	}

}
