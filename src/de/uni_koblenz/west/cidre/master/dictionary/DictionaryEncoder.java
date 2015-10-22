package de.uni_koblenz.west.cidre.master.dictionary;

import java.io.Closeable;
import java.io.File;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
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
