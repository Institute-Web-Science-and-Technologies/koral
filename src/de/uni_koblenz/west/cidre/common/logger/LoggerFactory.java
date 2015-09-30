package de.uni_koblenz.west.cidre.common.logger;

import java.io.IOException;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;

public class LoggerFactory {

	public static Logger getJeromqLogger(Configuration conf,
			String[] currentServer, String name, String receiver) {
		Logger logger = getLogger(name, conf.getLoglevel());
		logger.addHandler(
				new JeromqStreamHandler(conf, currentServer, receiver));
		return logger;
	}

	public static Logger getCSVFileLogger(Configuration conf,
			String[] currentServer, String name) throws IOException {
		Logger logger = getLogger(name, conf.getLoglevel());
		logger.addHandler(new CSVFileHandler(conf, currentServer));
		return logger;
	}

	private static Logger getLogger(String name, Level logLevel) {
		Logger logger = Logger.getLogger(name);
		for (Handler handler : logger.getHandlers()) {
			if (handler instanceof ConsoleHandler) {
				logger.removeHandler(handler);
			}
		}
		logger.setLevel(logLevel);
		return logger;
	}

}
