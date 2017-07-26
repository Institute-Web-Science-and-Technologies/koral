/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License along with Koral. If not,
 * see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
package de.uni_koblenz.west.koral.common.config.impl;

import de.uni_koblenz.west.koral.common.config.ConfigurableSerializer;

/**
 * Provides methods to convert the field values of {@link Configuration} to the property values in
 * the configuration file.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */

public class ConfigurationSerializer implements ConfigurableSerializer {

  public String serializeMaster(Configuration conf) {
    String[] master = conf.getMaster();
    if (master[0] != null) {
      return master[0] + ":" + master[1];
    } else {
      return "";
    }
  }

  public String serializeFtpServer(Configuration conf) {
    String[] ftpServer = conf.getFTPServer();
    if (ftpServer[0] != null) {
      return ftpServer[0] + ":" + ftpServer[1];
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

  public String serializeTmpDir(Configuration conf) {
    return conf.getTmpDir();
  }

  public String serializeDataDir(Configuration conf) {
    return conf.getDataDir();
  }

  public String serializeMaxDictionaryWriteBatchSize(Configuration conf) {
    return new Integer(conf.getMaxDictionaryWriteBatchSize()).toString();
  }

  public String serializeTripleStoreStorageType(Configuration conf) {
    return conf.getTripleStoreStorageType().name();
  }

  public String serializeEnableTransactionsForTripleStore(Configuration conf) {
    return new Boolean(conf.useTransactionsForTripleStore()).toString();
  }

  public String serializeEnableAsynchronousWritesForTripleStore(Configuration conf) {
    return new Boolean(conf.isTripleStoreAsynchronouslyWritten()).toString();
  }

  public String serializeTripleStoreCacheType(Configuration conf) {
    return conf.getTripleStoreCacheType().name();
  }

  public String serializeSizeOfMappingRecycleCache(Configuration conf) {
    return new Integer(conf.getSizeOfMappingRecycleCache()).toString();
  }

  public String serializeUnbalanceThresholdForWorkerThreads(Configuration conf) {
    return new Double(conf.getUnbalanceThresholdForWorkerThreads()).toString();
  }

  public String serializeMappingBundleSize(Configuration conf) {
    return new Integer(conf.getMappingBundleSize()).toString();
  }

  public String serializeReceiverQueueSize(Configuration conf) {
    return new Integer(conf.getReceiverQueueSize()).toString();
  }

  public String serializeMappingsPerOperationRound(Configuration conf) {
    return new Integer(conf.getMaxEmittedMappingsPerRound()).toString();
  }

  public String serializeJoinCacheStorageType(Configuration conf) {
    return conf.getJoinCacheStorageType().name();
  }

  public String serializeEnableTransactionsForJoinCache(Configuration conf) {
    return new Boolean(conf.useTransactionsForJoinCache()).toString();
  }

  public String serializeEnableAsynchronousWritesForJoinCache(Configuration conf) {
    return new Boolean(conf.isJoinCacheAsynchronouslyWritten()).toString();
  }

  public String serializeJoinCacheType(Configuration conf) {
    return conf.getJoinCacheType().name();
  }

}
