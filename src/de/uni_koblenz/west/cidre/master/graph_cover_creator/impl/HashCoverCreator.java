package de.uni_koblenz.west.cidre.master.graph_cover_creator.impl;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;

import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;
import de.uni_koblenz.west.cidre.master.graph_cover_creator.GraphCoverCreator;

public class HashCoverCreator implements GraphCoverCreator {

	private final Logger logger;

	private final MessageDigest digest;

	public HashCoverCreator(Logger logger) {
		this.logger = logger;
		try {
			digest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			if (logger != null) {
				logger.throwing(e.getStackTrace()[0].getClassName(),
						e.getStackTrace()[0].getMethodName(), e);
			}
			throw new RuntimeException(e);
		}
	}

	@Override
	public File[] createGraphCover(RDFFileIterator rdfFiles, File workingDir,
			int numberOfGraphChunks) {
		DatasetGraph graph = DatasetGraphFactory.createMem();
		File[] chunkFiles = getGraphChunkFiles(workingDir, numberOfGraphChunks);
		OutputStream[] outputs = getOutputStreams(chunkFiles);
		try {
			for (Node[] statement : rdfFiles) {
				transformBlankNodes(statement);
				// assign to triple to chunk according to hash on subject
				String subjectString = statement[0].toString();
				int targetChunk = computeHash(subjectString) % outputs.length;
				if (targetChunk < 0) {
					targetChunk *= -1;
				}

				if (statement.length == 3) {
					graph.getDefaultGraph().add(new Triple(statement[0],
							statement[1], statement[2]));
				} else {
					graph.add(new Quad(statement[3], statement[0], statement[1],
							statement[2]));
				}
				RDFDataMgr.write(outputs[targetChunk], graph, RDFFormat.NQ);
				graph.clear();
			}
		} finally {
			rdfFiles.close();
			for (OutputStream stream : outputs) {
				try {
					if (stream != null) {
						stream.close();
					}
				} catch (IOException e) {
				}
			}
		}
		return chunkFiles;
	}

	private int computeHash(String string) {
		byte[] hash = null;
		try {
			hash = digest.digest(string.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			if (logger != null) {
				logger.throwing(e.getStackTrace()[0].getClassName(),
						e.getStackTrace()[0].getMethodName(), e);
			}
			throw new RuntimeException(e);
		} finally {
			digest.reset();
		}
		int result = 0;
		for (int i = 0; i < hash.length; i += 4) {
			result ^= ByteBuffer
					.wrap(hash, i, i + 4 < hash.length ? 4 : hash.length - i)
					.getInt();
		}
		return result;
	}

	private void transformBlankNodes(Node[] statement) {
		for (int i = 0; i < statement.length; i++) {
			Node node = statement[i];
			if (node.isBlank()) {
				statement[i] = NodeFactory
						.createURI("urn:uuid:" + node.getBlankNodeId());
			}
		}
	}

	private OutputStream[] getOutputStreams(File[] chunkFiles) {
		OutputStream[] outputs = new OutputStream[chunkFiles.length];
		for (int i = 0; i < outputs.length; i++) {
			try {
				outputs[i] = new BufferedOutputStream(new GZIPOutputStream(
						new FileOutputStream(chunkFiles[i])));
			} catch (IOException e) {
				for (int j = i; i >= 0; j--) {
					if (outputs[j] != null) {
						try {
							outputs[j].close();
						} catch (IOException e1) {
						}
					}
				}
				throw new RuntimeException(e);
			}
		}
		return outputs;
	}

	private File[] getGraphChunkFiles(File workingDir,
			int numberOfGraphChunks) {
		File[] chunkFiles = new File[numberOfGraphChunks];
		for (int i = 0; i < chunkFiles.length; i++) {
			chunkFiles[i] = new File(workingDir.getAbsolutePath()
					+ File.separatorChar + "chunk" + i + ".nq.gz");
		}
		return chunkFiles;
	}

}
