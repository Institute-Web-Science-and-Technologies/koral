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
 * Provides methods to convert the field values of {@link Configuration} to the
 * property values in the configuration file.
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
    return Long.valueOf(conf.getClientConnectionTimeout()).toString();
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
    return Integer.valueOf(conf.getMaxDictionaryWriteBatchSize()).toString();
  }

  public String serializeEnableTransactionsForTripleStore(Configuration conf) {
    return Boolean.valueOf(conf.useTransactionsForTripleStore()).toString();
  }

  public String serializeSizeOfMappingRecycleCache(Configuration conf) {
    return Integer.valueOf(conf.getSizeOfMappingRecycleCache()).toString();
  }

  public String serializeUnbalanceThresholdForWorkerThreads(Configuration conf) {
    return Double.valueOf(conf.getUnbalanceThresholdForWorkerThreads()).toString();
  }

  public String serializeMappingBundleSize(Configuration conf) {
    return Integer.valueOf(conf.getMappingBundleSize()).toString();
  }

  public String serializeReceiverQueueSize(Configuration conf) {
    return Integer.valueOf(conf.getReceiverQueueSize()).toString();
  }

  public String serializeMappingsPerOperationRound(Configuration conf) {
    return Integer.valueOf(conf.getMaxEmittedMappingsPerRound()).toString();
  }

  public String serializeEnableTransactionsForJoinCache(Configuration conf) {
    return Boolean.valueOf(conf.useTransactionsForJoinCache()).toString();
  }

  public String serializeRowDataLength(Configuration conf) {
    return Integer.valueOf(conf.getRowDataLength()).toString();
  }

  public String serializeIndexCacheSize(Configuration conf) {
    return Integer.valueOf(conf.getIndexCacheSize()).toString();
  }

  public String serializeExtraCacheSize(Configuration conf) {
    return Integer.valueOf(conf.getExtraCacheSize()).toString();
  }

  public String serializeRecyclerCapacity(Configuration conf) {
    return Integer.valueOf(conf.getRecyclerCapacity()).toString();
  }

  public String serializeBlockSize(Configuration conf) {
    return Integer.valueOf(conf.getBlockSize()).toString();
  }

  public String serializeMaxOpenFiles(Configuration conf) {
    return Integer.valueOf(conf.getMaxOpenFiles()).toString();
  }
}
