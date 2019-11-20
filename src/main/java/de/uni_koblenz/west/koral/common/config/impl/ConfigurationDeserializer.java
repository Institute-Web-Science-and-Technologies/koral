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

import java.util.logging.Level;
import java.util.regex.Pattern;

import de.uni_koblenz.west.koral.common.config.ConfigurableDeserializer;

/**
 * Provides methods to convert the property values in the configuration file to the field values of
 * {@link Configuration}.
 *
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class ConfigurationDeserializer implements ConfigurableDeserializer {

  public void deserializeMaster(Configuration conf, String master) {
    if (master.indexOf(':') == -1) {
      conf.setMaster(master);
    } else {
      String[] parts = master.split(Pattern.quote(":"));
      conf.setMaster(parts[0], parts[1]);
    }
  }

  public void deserializeFtpServer(Configuration conf, String ftpServer) {
    if (ftpServer.indexOf(':') == -1) {
      conf.setFTPServer(ftpServer);
    } else {
      String[] parts = ftpServer.split(Pattern.quote(":"));
      conf.setFTPServer(parts[0], parts[1]);
    }
  }

  public void deserializeSlaves(Configuration conf, String slaves) {
    String[] entries = slaves.split(Pattern.quote(","));
    for (int i = 0; i < entries.length; i++) {
      String entry = entries[i];
      if (entry.indexOf(':') == -1) {
        conf.addSlave(entry);
      } else {
        String[] parts = entry.split(Pattern.quote(":"));
        conf.addSlave(parts[0], parts[1]);
      }
    }
  }

  public void deserializeClientConnection(Configuration conf, String client) {
    if (client.indexOf(':') == -1) {
      conf.setClient(client);
    } else {
      String[] parts = client.split(Pattern.quote(":"));
      conf.setClient(parts[0], parts[1]);
    }
  }

  public void deserializeClientConnectionTimeout(Configuration conf,
      String clientConnectionTimeout) {
    conf.setClientConnectionTimeout(Long.parseLong(clientConnectionTimeout));
  }

  public void deserializeLogLevel(Configuration conf, String logLevel) {
    conf.setLoglevel(Level.parse(logLevel));
  }

  public void deserializeTmpDir(Configuration conf, String tmpDir) {
    if ((tmpDir != null) && !tmpDir.isEmpty()) {
      conf.setTmpDir(tmpDir);
    }
  }

  public void deserializeDataDir(Configuration conf, String dataDir) {
    if ((dataDir != null) && !dataDir.isEmpty()) {
      conf.setDataDir(dataDir);
    }
  }

  public void deserializeMaxDictionaryWriteBatchSize(Configuration conf,
      String maxDictionaryWriteBatchSize) {
    if ((maxDictionaryWriteBatchSize != null) && !maxDictionaryWriteBatchSize.isEmpty()) {
      conf.setMaxDictionaryWriteBatchSize(Integer.parseInt(maxDictionaryWriteBatchSize));
    }
  }

  public void deserializeEnableTransactionsForTripleStore(Configuration conf,
      String enableTransactions) {
    if ((enableTransactions != null) && !enableTransactions.isEmpty()) {
      conf.setUseTransactionsForTripleStore(Boolean.parseBoolean(enableTransactions));
    }
  }

  public void deserializeSizeOfMappingRecycleCache(Configuration conf, String size) {
    conf.setSizeOfMappingRecycleCache(Integer.parseInt(size));
  }

  public void deserializeUnbalanceThresholdForWorkerThreads(Configuration conf, String threshold) {
    conf.setUnbalanceThresholdForWorkerThreads(Double.parseDouble(threshold));
  }

  public void deserializeMappingBundleSize(Configuration conf, String size) {
    conf.setMappingBundleSize(Integer.parseInt(size));
  }

  public void deserializeReceiverQueueSize(Configuration conf, String size) {
    conf.setReceiverQueueSize(Integer.parseInt(size));
  }

  public void deserializeMappingsPerOperationRound(Configuration conf, String mappingsPerRound) {
    conf.setMaxEmittedMappingsPerRound(Integer.parseInt(mappingsPerRound));
  }

  public void deserializeEnableTransactionsForJoinCache(Configuration conf,
      String enableTransactions) {
    if ((enableTransactions != null) && !enableTransactions.isEmpty()) {
      conf.setUseTransactionsForJoinCache(Boolean.parseBoolean(enableTransactions));
    }
  }
  
  public void deserializeRowDataLength(Configuration conf, String rowDataLength) {
	  conf.setRowDataLength(Integer.parseInt(rowDataLength));
  }
  
  public void deserializeIndexCacheSize(Configuration conf, String indexCacheSize) {
	  conf.setIndexCacheSize(Integer.parseInt(indexCacheSize));
  }
  
  public void deserializeExtraCacheSize(Configuration conf, String extraCacheSize) {
	  conf.setExtraCacheSize(Integer.parseInt(extraCacheSize));
  }
  
  public void deserializeRecyclerCapacity(Configuration conf, String recyclerCapacity) {
	  conf.setRecyclerCapacity(Integer.parseInt(recyclerCapacity));
  }
  
  public void deserializeBlockSize(Configuration conf, String blockSize) {
	  conf.setBlockSize(Integer.parseInt(blockSize));
  }
  
  public void deserializeMaxOpenFiles(Configuration conf, String maxOpenFiles) {
	  conf.setMaxOpenFiles(Integer.parseInt(maxOpenFiles));
  }

}
