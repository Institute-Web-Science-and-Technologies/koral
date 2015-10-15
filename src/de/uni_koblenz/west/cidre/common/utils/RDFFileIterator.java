package de.uni_koblenz.west.cidre.common.utils;

import java.io.File;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RiotException;

public class RDFFileIterator
		implements Iterable<Statement>, Iterator<Statement> {

	private final Logger logger;

	private final File[] rdfFiles;

	private int currentFile;

	private StmtIterator iterator;

	public RDFFileIterator(File file, Logger logger) {
		this.logger = logger;
		GraphFileFilter filter = new GraphFileFilter();
		if (file.exists() && file.isFile() && filter.accept(file)) {
			rdfFiles = new File[] { file };
		} else if (file.exists() && file.isDirectory()) {
			rdfFiles = file.listFiles(filter);
		} else {
			rdfFiles = new File[0];
		}
		getNextIterator();
	}

	private void getNextIterator() {
		if (currentFile >= rdfFiles.length) {
			iterator = null;
		}
		RiotException e = null;
		do {
			try {
				iterator = null;
				e = null;
				Model model = RDFDataMgr
						.loadModel(rdfFiles[currentFile++].getAbsolutePath());
				iterator = model.listStatements();
			} catch (RiotException e1) {
				if (logger != null) {
					logger.finer("Skipping file "
							+ rdfFiles[currentFile - 1].getAbsolutePath()
							+ " because of the following error.");
					logger.throwing(e1.getStackTrace()[0].getClassName(),
							e1.getStackTrace()[0].getMethodName(), e1);
				}
				e = e1;
			}
		} while (currentFile < rdfFiles.length && e != null);
	}

	@Override
	public boolean hasNext() {
		return iterator != null && iterator.hasNext();
	}

	@Override
	public Statement next() {
		Statement next = iterator.next();
		if (!iterator.hasNext()) {
			getNextIterator();
		}
		return next;
	}

	@Override
	public Iterator<Statement> iterator() {
		return this;
	}

}
