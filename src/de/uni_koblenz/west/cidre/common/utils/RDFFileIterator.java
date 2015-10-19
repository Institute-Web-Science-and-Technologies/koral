package de.uni_koblenz.west.cidre.common.utils;

import java.io.File;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.lang.PipedQuadsStream;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.apache.jena.sparql.core.Quad;

public class RDFFileIterator implements Iterable<Node[]>, Iterator<Node[]> {

	private final Logger logger;

	private final File[] rdfFiles;

	private int currentFile;

	private PipedRDFIterator<?> iterator;

	private boolean isQuad;

	private final ExecutorService executor;

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
		executor = Executors.newSingleThreadExecutor();
		getNextIterator();
	}

	private void getNextIterator() {
		if (currentFile >= rdfFiles.length) {
			iterator = null;
			return;
		}

		iterator = new PipedRDFIterator<>();
		Lang lang = RDFLanguages
				.filenameToLang(rdfFiles[currentFile].getName());
		isQuad = RDFLanguages.isQuads(lang);
		@SuppressWarnings("unchecked")
		PipedRDFStream<?> inputStream = isQuad
				? new PipedQuadsStream((PipedRDFIterator<Quad>) iterator)
				: new PipedTriplesStream((PipedRDFIterator<Triple>) iterator);

		Runnable parser = new Runnable() {
			@Override
			public void run() {
				try {
					RDFDataMgr.parse(inputStream,
							rdfFiles[currentFile++].getAbsolutePath());
				} catch (RiotException e) {
					System.err.println(
							rdfFiles[currentFile - 1].getAbsolutePath());
					e.printStackTrace();
					if (logger != null) {
						logger.finer("Skipping rest of file "
								+ rdfFiles[currentFile - 1].getAbsolutePath()
								+ " because of the following error.");
						logger.throwing(e.getStackTrace()[0].getClassName(),
								e.getStackTrace()[0].getMethodName(), e);
					}
				}
			}
		};

		executor.submit(parser);
	}

	@Override
	public boolean hasNext() {
		return iterator != null && iterator.hasNext();
	}

	@Override
	public Node[] next() {
		Node[] next = null;
		if (isQuad) {
			Quad quad = (Quad) iterator.next();
			if (Quad.isDefaultGraphGenerated(quad.getGraph())) {
				next = new Node[3];
			} else {
				next = new Node[4];
				next[3] = quad.getGraph();
			}
			next[0] = quad.getSubject();
			next[1] = quad.getPredicate();
			next[2] = quad.getObject();
		} else {
			Triple triple = (Triple) iterator.next();
			next = new Node[3];
			next[0] = triple.getSubject();
			next[1] = triple.getPredicate();
			next[2] = triple.getObject();
		}
		if (!iterator.hasNext()) {
			getNextIterator();
		}
		if (!hasNext()) {
			executor.shutdown();
		}
		return next;
	}

	@Override
	public Iterator<Node[]> iterator() {
		return this;
	}

}
