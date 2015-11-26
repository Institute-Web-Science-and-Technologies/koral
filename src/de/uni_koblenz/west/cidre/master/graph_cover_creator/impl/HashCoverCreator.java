package de.uni_koblenz.west.cidre.master.graph_cover_creator.impl;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;

import de.uni_koblenz.west.cidre.common.utils.NumberConversion;
import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;
import de.uni_koblenz.west.cidre.master.graph_cover_creator.GraphCoverCreator;
import de.uni_koblenz.west.cidre.master.utils.DeSerializer;

/**
 * Creates a hash cover based on the subject of the triples.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
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
		boolean[] writtenFiles = new boolean[chunkFiles.length];
		try {
			for (Node[] statement : rdfFiles) {
				transformBlankNodes(statement);
				// assign to triple to chunk according to hash on subject
				String subjectString = statement[0].toString();
				int targetChunk = computeHash(subjectString) % outputs.length;
				if (targetChunk < 0) {
					targetChunk *= -1;
				}

				// ignore graphs and add all triples to the same graph
				// encode the containment information as graph name
				graph.add(new Quad(
						encodeContainmentInformation(targetChunk,
								numberOfGraphChunks),
						statement[0], statement[1], statement[2]));
				RDFDataMgr.write(outputs[targetChunk], graph, RDFFormat.NQ);
				graph.clear();
				writtenFiles[targetChunk] = true;
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
			// delete empty chunks
			for (int i = 0; i < chunkFiles.length; i++) {
				if (!writtenFiles[i]) {
					if (chunkFiles[i] != null && chunkFiles[i].exists()) {
						chunkFiles[i].delete();
						chunkFiles[i] = null;
					}
				}
			}
		}
		return chunkFiles;
	}

	private Node encodeContainmentInformation(int targetChunk,
			int numberOfGraphChunks) {
		int bitsetSize = numberOfGraphChunks / Byte.SIZE;
		if (numberOfGraphChunks % Byte.SIZE != 0) {
			bitsetSize += 1;
		}
		byte[] bitset = new byte[bitsetSize];
		int bitsetIndex = targetChunk / Byte.SIZE;
		byte bitsetMask = getBitMaskFor(targetChunk);
		bitset[bitsetIndex] |= bitsetMask;
		return DeSerializer.serializeBitSetAsNode(bitset);
	}

	private byte getBitMaskFor(int computerId) {
		switch (computerId % Byte.SIZE) {
		case 0:
			return (byte) 0x80;
		case 1:
			return (byte) 0x40;
		case 2:
			return (byte) 0x20;
		case 3:
			return (byte) 0x10;
		case 4:
			return (byte) 0x08;
		case 5:
			return (byte) 0x04;
		case 6:
			return (byte) 0x02;
		case 7:
			return (byte) 0x01;
		}
		return 0;
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
			if (i + 3 < hash.length) {
				result ^= NumberConversion.bytes2int(hash, i);
			} else {
				while (i < hash.length) {
					result ^= hash[i];
					i++;
				}
			}
		}
		return result;
	}

	private void transformBlankNodes(Node[] statement) {
		for (int i = 0; i < statement.length; i++) {
			Node node = statement[i];
			if (node.isBlank()) {
				statement[i] = NodeFactory
						.createURI("urn:blankNode:" + node.getBlankNodeId());
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
