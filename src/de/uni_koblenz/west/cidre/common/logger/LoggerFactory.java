package de.uni_koblenz.west.cidre.common.logger;

import java.util.logging.Level;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.config.Configuration;

public class LoggerFactory {

	public static Logger getJeromqLogger(Configuration conf,
			String[] currentServer, String name, String receiver) {
		Logger logger = getLogger(name);
		logger.addHandler(
				new JeromqStreamHandler(conf, currentServer, receiver));
		return logger;
	}

	private static Logger getLogger(String name) {
		Logger logger = Logger.getLogger(name);
		logger.setLevel(Level.ALL);
		return logger;
	}

}
