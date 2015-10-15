package de.uni_koblenz.west.cidre.master.graph_cover_creator.impl;

import java.io.File;
import java.util.logging.Logger;

import org.apache.jena.rdf.model.Statement;

import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;
import de.uni_koblenz.west.cidre.master.graph_cover_creator.GraphCoverCreator;

public class HashCoverCreator implements GraphCoverCreator {

	private final Logger logger;

	public HashCoverCreator(Logger logger) {
		this.logger = logger;
	}

	@Override
	public File[] createGraphCover(RDFFileIterator rdfFiles, File workingDir,
			int numberOfGraphChunks) {
		for (Statement statement : rdfFiles) {
			if (logger != null) {
				logger.finest("\n" + statement.getSubject() + " "
						+ statement.getPredicate() + " " + statement.getObject()
						+ "\n");
			}
		}
		// TODO Auto-generated method stub
		return null;
	}

}
