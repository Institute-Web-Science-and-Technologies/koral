package de.uni_koblenz.west.cidre.common.config.impl;

import de.uni_koblenz.west.cidre.common.config.ConfigurableSerializer;

public class ConfigurationSerializer implements ConfigurableSerializer {

	public String serializeMaster(Configuration conf) {
		String[] master = conf.getMaster();
		if (master[0] != null) {
			return master[0] + ":" + master[1];
		} else {
			return "";
		}
	}

	public String serializeSlaves(Configuration conf) {
		StringBuilder sb = new StringBuilder();
		String delim = "";
		for (int i = 0; i < conf.getNumberOfSlaves(); i++) {
			String[] slave = conf.getSlave(i);
			sb.append(delim).append(slave[0]).append(":").append(slave[1]);
			delim = ",";
		}
		return sb.toString();
	}

	public String serializeClientConnection(Configuration conf) {
		String[] client = conf.getClient();
		if (client[0] != null) {
			return client[0] + ":" + client[1];
		} else {
			return "";
		}
	}

	public String serializeClientConnectionTimeout(Configuration conf) {
		return new Long(conf.getClientConnectionTimeout()).toString();
	}

	public String serializeLogLevel(Configuration conf) {
		return conf.getLoglevel().getName();
	}

	public String serializeLoggingDirectory(Configuration conf) {
		return conf.getLogDirectory();
	}

	public String serializeTmpDir(Configuration conf) {
		return conf.getTmpDir();
	}

	public String serializeDictionaryStorageType(Configuration conf) {
		return conf.getDictionaryStorageType().name();
	}

	public String serializeDictionaryDataStructure(Configuration conf) {
		return conf.getDictionaryDataStructure().name();
	}

	public String serializeDictionaryDir(Configuration conf) {
		return conf.getDictionaryDir();
	}

	public String serializeEnableTransactionsForDictionary(Configuration conf) {
		return new Boolean(conf.useTransactionsForDictionary()).toString();
	}

	public String serializeEnableAsynchronousWritesForDictionary(
			Configuration conf) {
		return new Boolean(conf.isDictionaryAsynchronouslyWritten()).toString();
	}

	public String serializeDictionaryCacheType(Configuration conf) {
		return conf.getDictionaryCacheType().name();
	}

	public String serializeStatisticsStorageType(Configuration conf) {
		return conf.getStatisticsStorageType().name();
	}

	public String serializeStatisticsDataStructure(Configuration conf) {
		return conf.getStatisticsDataStructure().name();
	}

	public String serializeStatisticsDir(Configuration conf) {
		return conf.getStatisticsDir();
	}

	public String serializeEnableTransactionsForStatistics(Configuration conf) {
		return new Boolean(conf.useTransactionsForStatistics()).toString();
	}

	public String serializeEnableAsynchronousWritesForStatistics(
			Configuration conf) {
		return new Boolean(conf.areStatisticsAsynchronouslyWritten())
				.toString();
	}

	public String serializeStatisticsCacheType(Configuration conf) {
		return conf.getStatisticsCacheType().name();
	}

	public String serializeTripleStoreStorageType(Configuration conf) {
		return conf.getTripleStoreStorageType().name();
	}

	public String serializeTripleStoreDir(Configuration conf) {
		return conf.getTripleStoreDir();
	}

	public String serializeEnableTransactionsForTripleStore(
			Configuration conf) {
		return new Boolean(conf.useTransactionsForTripleStore()).toString();
	}

	public String serializeEnableAsynchronousWritesForTripleStore(
			Configuration conf) {
		return new Boolean(conf.isTripleStoreAsynchronouslyWritten())
				.toString();
	}

	public String serializeTripleStoreCacheType(Configuration conf) {
		return conf.getTripleStoreCacheType().name();
	}

	public String serializeSizeOfMappingRecycleCache(Configuration conf) {
		return new Integer(conf.getSizeOfMappingRecycleCache()).toString();
	}

	public String serializeUnbalanceThresholdForWorkerThreads(
			Configuration conf) {
		return new Double(conf.getUnbalanceThresholdForWorkerThreads())
				.toString();
	}

}
