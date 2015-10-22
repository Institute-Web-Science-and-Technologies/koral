package de.uni_koblenz.west.cidre.master.dictionary;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import org.apache.jena.graph.Node;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;
import de.uni_koblenz.west.cidre.master.dictionary.impl.MapDBDictionary;
import de.uni_koblenz.west.cidre.master.statisticsDB.GraphStatistics;

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
					long subject = dictionary.encode(quad[0].toString());
					long property = dictionary.encode(quad[1].toString());
					long object = dictionary.encode(quad[2].toString());
					statistics.count(subject, property, object, i);
					byte[] containment;
					// TODO Auto-generated method stub
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

	public void clear() {
		dictionary.clear();
	}

	@Override
	public void close() {
		dictionary.close();
	}

}
