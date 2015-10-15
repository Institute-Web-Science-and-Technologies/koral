package de.uni_koblenz.west.cidre.master.dictionary;

import java.io.File;
import java.util.logging.Logger;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.RDFDataMgr;

public class Dictionary {

	private final Logger logger;

	public Dictionary(Logger logger) {
		this.logger = logger;
	}

	public File[] encode(File... files) {
		for (File file : files) {
			if (logger != null) {
				logger.finest("encoding " + file);
			}
			Model model = RDFDataMgr.loadModel(file.getAbsolutePath());
			StmtIterator it = model.listStatements();
			if (!it.hasNext() && logger != null) {
				logger.finest(file.getAbsolutePath() + " has no content.");
			}
			while (it.hasNext()) {
				Statement statement = it.next();
				Resource subject = statement.getSubject();
				Property predicate = statement.getPredicate();
				RDFNode object = statement.getObject();
				if (logger != null) {
					logger.info(subject + " " + predicate + " " + object);
				}
			}
		}
		return null;
	}

}
