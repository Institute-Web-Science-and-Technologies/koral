package de.uni_koblenz.west.cidre.master.dictionary;

import java.io.File;
import java.util.logging.Logger;

import org.apache.jena.riot.RiotException;

public class Dictionary {

	private final Logger logger;

	public Dictionary(Logger logger) {
		this.logger = logger;
	}

	public File encode(File file) {
		if (logger != null) {
			logger.finest("encoding " + file);
		}
		try {
			// TODO implement
		} catch (RiotException e) {
			if (logger != null) {
				logger.throwing(e.getStackTrace()[0].getClassName(),
						e.getStackTrace()[0].getMethodName(), e);
			}
			throw new RuntimeException(e);
		}
		return null;
	}

}
