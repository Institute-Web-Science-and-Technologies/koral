package de.uni_koblenz.west.cidre.common.utils;

import java.io.Closeable;
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

/**
 * Blank nodes get an id unique to the computer on which the graph file is read
 * first.
 */
public class RDFFileIterator
		implements Iterable<Node[]>, Iterator<Node[]>, Closeable {

	private final Logger logger;

	private final File[] rdfFiles;

	private int currentFile;

	private PipedRDFIterator<?> iterator;

	private boolean isQuad;

	private final ExecutorService executor;

	private final boolean deleteReadFiles;

	public RDFFileIterator(File file, Logger logger) {
		this(file, true, logger);
	}

	public RDFFileIterator(File file, boolean deleteFiles, Logger logger) {
		this.logger = logger;
		deleteReadFiles = deleteFiles;
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
		if (deleteReadFiles && currentFile > 0
				&& currentFile <= rdfFiles.length) {
			rdfFiles[currentFile - 1].delete();
		}
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
			if (deleteReadFiles) {
				rdfFiles[rdfFiles.length - 1].delete();
			}
			close();
		}
		return next;
	}

	@Override
	public void close() {
		executor.shutdown();
	}

	@Override
	public Iterator<Node[]> iterator() {
		return this;
	}

}
