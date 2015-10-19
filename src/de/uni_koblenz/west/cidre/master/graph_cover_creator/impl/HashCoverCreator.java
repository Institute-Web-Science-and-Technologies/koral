package de.uni_koblenz.west.cidre.master.graph_cover_creator.impl;

import java.io.File;
import java.util.logging.Logger;

import org.apache.jena.graph.Node;

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
		for (Node[] statement : rdfFiles) {
			if (logger != null) {
				StringBuilder sb = new StringBuilder();
				String delim = "";
				for (Node node : statement) {
					sb.append(delim).append(node.toString(true));
					delim = " ";
				}
				logger.finest("\n" + sb.toString() + "\n");
			}
		}
		// TODO handle empty chunks
		return null;
	}

}