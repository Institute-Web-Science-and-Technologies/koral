package de.uni_koblenz.west.cidre.master.dictionary;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import org.apache.jena.graph.Node;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;
import de.uni_koblenz.west.cidre.master.dictionary.impl.MapDBDictionary;
import de.uni_koblenz.west.cidre.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.cidre.master.utils.DeSerializer;

/**
 * Triple Nodes are encoded using {@link DeSerializer}
 */
public class DictionaryEncoder implements Closeable {

	private final Logger logger;

	private final Dictionary dictionary;

	public DictionaryEncoder(Configuration conf, Logger logger) {
		this.logger = logger;
		dictionary = new MapDBDictionary(conf.getDictionaryStorageType(),
				conf.getDictionaryDataStructure(), conf.getDictionaryDir(),
				conf.useTransactionsForDictionary(),
				conf.isDictionaryAsynchronouslyWritten(),
				conf.getDictionaryCacheType());
	}

	public File[] encodeGraphChunks(File[] plainGraphChunks,
			GraphStatistics statistics) {
		File[] itermediateFiles = encodeGraphChunksAndCountStatistics(
				plainGraphChunks, statistics);
		return setOwnership(itermediateFiles, statistics);
	}

	private File[] encodeGraphChunksAndCountStatistics(File[] plainGraphChunks,
			GraphStatistics statistics) {
		File[] intermediateFiles = new File[plainGraphChunks.length];
		for (int i = 0; i < plainGraphChunks.length; i++) {
			if (plainGraphChunks[i] == null) {
				continue;
			}
			intermediateFiles[i] = new File(
					plainGraphChunks[i].getParentFile().getAbsolutePath()
							+ File.separatorChar + "chunk" + i + ".enc.int.gz");
			try (RDFFileIterator iter = new RDFFileIterator(plainGraphChunks[i],
					logger);
					OutputStream out = new BufferedOutputStream(
							new GZIPOutputStream(new FileOutputStream(
									intermediateFiles[i])));) {
				for (Node[] quad : iter) {
					long subject = dictionary
							.encode(DeSerializer.serializeNode(quad[0]));
					long property = dictionary
							.encode(DeSerializer.serializeNode(quad[1]));
					long object = dictionary
							.encode(DeSerializer.serializeNode(quad[2]));
					statistics.count(subject, property, object, i);
					byte[] containment = DeSerializer
							.deserializeBitSetFromNode(quad[3]).toByteArray();
					out.write(ByteBuffer.allocate(8).putLong(subject).array());
					out.write(ByteBuffer.allocate(8).putLong(property).array());
					out.write(ByteBuffer.allocate(8).putLong(object).array());
					out.write(ByteBuffer.allocate(2)
							.putShort((short) containment.length).array());
					out.write(containment);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return intermediateFiles;
	}

	private File[] setOwnership(File[] intermediateFiles,
			GraphStatistics statistics) {
		// TODO Auto-generated method stub
		return null;
	}

	public Node decode(long id) {
		String plainText = dictionary.decode(id);
		if (plainText == null) {
			return null;
		}
		return DeSerializer.deserializeNode(plainText);
	}

	public void clear() {
		dictionary.clear();
	}

	@Override
	public void close() {
		dictionary.close();
	}

}
