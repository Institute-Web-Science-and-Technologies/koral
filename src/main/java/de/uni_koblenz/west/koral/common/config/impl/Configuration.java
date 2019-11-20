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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import de.uni_koblenz.west.koral.common.config.Configurable;
import de.uni_koblenz.west.koral.common.config.ConfigurableDeserializer;
import de.uni_koblenz.west.koral.common.config.ConfigurableSerializer;
import de.uni_koblenz.west.koral.common.config.Property;
import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.system.ConfigurationException;
import de.uni_koblenz.west.koral.master.dictionary.impl.RocksDBDictionary;

/**
 * Contains all configuration options for Koral. Options that are serialized in the configuration
 * file are annotated with {@link Property}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class Configuration implements Configurable {

  public static final String QUERY_RESULT_COLUMN_SEPARATOR_CHAR = "\t";

  public static final String QUERY_RESULT_ROW_SEPARATOR_CHAR = "\n";

  public static final String BLANK_NODE_URI_PREFIX = "urn:blankNode:";

  private static final String DEFAULT_PORT = "4710";

  private static final String DEFAULT_FTP_PORT = "2121";

  /**
   * Returns a string of the subfolder of the tmp / data folder. The subfolder is either master or
   * slave + number.
   * 
   * @return {@link String} string
   */
  private String getSubFolder() {
    if (currentSlave == 0) {
      return "master";
    }
    return "slave" + currentSlave;
  }

  @Property(name = "master",
      description = "The ip and port of the master server, e.g., 192.168.0.1:4710. If no port is specified, the default port "
          + Configuration.DEFAULT_PORT + " is used.")
  private String masterIP;

  private String masterPort;

  public String[] getMaster() {
    return new String[] {masterIP, masterPort};
  }

  public void setMaster(String masterIP) {
    setMaster(masterIP, Configuration.DEFAULT_PORT);
  }

  public void setMaster(String masterIP, String masterPort) {
    this.masterIP = masterIP;
    this.masterPort = masterPort;
  }

  @Property(name = "ftpServer",
      description = "The external ip and the internal and external port of the FTP server started at the master server, e.g., 192.168.0.1:2121."
          + " If no port is specified, the default port " + Configuration.DEFAULT_FTP_PORT
          + " is used."
          + " The FTP server is used to upload the graph files from the client to the master and the graph chunks from the master to the slaves."
          + " The FTP server runs only during the filetransfer.")
  private String ftpServerIP;

  private String ftpServerPort;

  public String[] getFTPServer() {
    return new String[] {ftpServerIP, ftpServerPort};
  }

  public void setFTPServer(String ftpServerIP) {
    setFTPServer(ftpServerIP, Configuration.DEFAULT_FTP_PORT);
  }

  public void setFTPServer(String ftpServerIP, String ftpServerPort) {
    this.ftpServerIP = ftpServerIP;
    this.ftpServerPort = ftpServerPort;
  }

  @Property(name = "slaves",
      description = "The comma separated list of ips and ports of the different slaves, e.g., 192.168.0.2:4712,192.168.0.3,192.168.0.4:4777. If no port is specified, the default port "
          + Configuration.DEFAULT_PORT + " is used.")
  private List<String> slaveIPs;

  private List<String> slavePorts;

  /**
   * Current master / slave. If value of zero(0), then its the master, else its the slave. Pay
   * attention: value n means slave (n - 1).
   */
  private int currentSlave = 0;

  private void findCurrentSlaveIpPort() throws ConfigurationException {
    // check if method has already been called
    if (currentSlave > 0) {
      return;
    }
    for (int i = 0; i < getNumberOfSlaves(); i++) {
      String[] slave = getSlave(i);
      try {
        NetworkInterface ni = NetworkInterface.getByInetAddress(InetAddress.getByName(slave[0]));
        if (ni != null) {
          // checking if port is free
          // if port is not free, check if next slave would have the same ip
          // to prevent reinitializing NetworkInterface
          boolean flagCheckPort = true;
          while (flagCheckPort) {
            try {
              new ServerSocket(Integer.valueOf(slave[1])).close();
              // true, port is free
              currentSlave = i + 1;
              return;
            } catch (IOException e) {
              // false, port is not free
            }
            if ((i + 1) < getNumberOfSlaves()) {
              String[] nextSlave = getSlave(i + 1);
              if (slave[0] != nextSlave[0]) {
                flagCheckPort = false;
              } else {
                slave = nextSlave;
                i++;
              }
            } else {
              flagCheckPort = false;
            }
          }
        }
      } catch (SocketException | UnknownHostException e) {
      }
    }
    throw new ConfigurationException(
        "The current slave cannot be found in the configuration file.");
  }

  public String[] getSlave(int index) {
    return new String[] {slaveIPs.get(index), slavePorts.get(index)};
  }

  public String[] getCurrentSlave() throws ConfigurationException {
    if (currentSlave == 0) {
      findCurrentSlaveIpPort();
    }
    if (currentSlave == 0) { // its a master, not a slave
      throw new ConfigurationException(
          "The current koral system is configured as master, not as slave.");
    }
    return new String[] {slaveIPs.get(currentSlave - 1), slavePorts.get(currentSlave - 1)};
  }

  public int getNumberOfSlaves() {
    return slaveIPs == null ? 0 : slaveIPs.size();
  }

  public void addSlave(String slaveIP) {
    addSlave(slaveIP, Configuration.DEFAULT_PORT);
  }

  public void addSlave(String slaveIP, String slavePort) {
    if (slaveIPs == null) {
      slaveIPs = new ArrayList<>();
      slavePorts = new ArrayList<>();
    }
    slaveIPs.add(slaveIP);
    slavePorts.add(slavePort);
  }

  public static final String DEFAULT_CLIENT_PORT = "4711";

  @Property(name = "clientConnection",
      description = "The ip and port to which clients can connect, e.g., 192.168.0.1:4711. If no port is specified, the default port "
          + Configuration.DEFAULT_CLIENT_PORT + " is used.")
  private String clientIP;

  private String clientPort;

  public String[] getClient() {
    return new String[] {clientIP, clientPort};
  }

  public void setClient(String clientIP) {
    setClient(clientIP, Configuration.DEFAULT_CLIENT_PORT);
  }

  public void setClient(String clientIP, String clientPort) {
    this.clientIP = clientIP;
    this.clientPort = clientPort;
  }

  public static final long CLIENT_CONNECTION_TIMEOUT = Long.MAX_VALUE;// 10000;

  public static final long CLIENT_KEEP_ALIVE_INTERVAL = 3000;

  @Property(name = "clientConnectionTimeout",
      description = "The number of milliseconds the master waits for messages from the client before closing the connection."
          + " Every " + Configuration.CLIENT_KEEP_ALIVE_INTERVAL
          + " milliseconds the client sends a keep alive message to the master. The default value is "
          + Configuration.CLIENT_CONNECTION_TIMEOUT + " milliseconds.")
  private long clientConnectionTimeout = Configuration.CLIENT_CONNECTION_TIMEOUT;

  public long getClientConnectionTimeout() {
    return clientConnectionTimeout;
  }

  public void setClientConnectionTimeout(long clientConnectionTimeout) {
    this.clientConnectionTimeout = clientConnectionTimeout;
  }

  private String romoteLoggerReceiver;

  public String getRomoteLoggerReceiver() {
    return romoteLoggerReceiver;
  }

  public void setRomoteLoggerReceiver(String romoteLoggerReceiver) {
    this.romoteLoggerReceiver = romoteLoggerReceiver;
  }

  private String romoteMeasurementReceiver;

  public String getRomoteMeasurementReceiver() {
    return romoteMeasurementReceiver;
  }

  public void setRomoteMeasurementReceiver(String romoteMeasurementReceiver) {
    this.romoteMeasurementReceiver = romoteMeasurementReceiver;
  }

  @Property(name = "logLevel",
      description = "Sets the logging level to one of: OFF, SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST, ALL")
  private Level loglevel = Level.INFO;

  private String logDirectory = "log";

  public Level getLoglevel() {
    return loglevel;
  }

  public void setLoglevel(Level loglevel) {
    this.loglevel = loglevel;
  }

  public String getLogDirectory(boolean flagIsMaster) {
    return getDataDirByInstance(flagIsMaster) + File.separatorChar + logDirectory;
  }

  @Property(name = "tmpDir",
      description = "Defines the directory where intermediate data is stored. Default directory (i.e., if not set) is the temporary directory of the operating system.")
  private String tmpDir = System.getProperty("java.io.tmpdir");

  public String getTmpDir() {
    return tmpDir;
  }

  public String getTmpDirByInstance(boolean flagIsMaster) {
    if (!(flagIsMaster)) {
      if (currentSlave == 0) {
        try {
          findCurrentSlaveIpPort();
        } catch (ConfigurationException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return tmpDir + File.separatorChar + getSubFolder();
  }

  public void setTmpDir(String tmpDir) {
    this.tmpDir = tmpDir;
  }

  @Property(name = "dataDir",
      description = "Defines the directory where data (e.g. triplestore, dictionary and statistics) is stored. Default directory (i.e., if not set) is the temporary directory of the operating system.")
  private String dataDir = System.getProperty("java.io.tmpdir");

  public void setDataDir(String dataDir) {
    this.dataDir = dataDir;
  }

  public String getDataDir() {
    return dataDir;
  }

  public String getDataDirByInstance(boolean flagIsMaster) {
    if (!(flagIsMaster)) {
      if (currentSlave == 0) {
        try {
          findCurrentSlaveIpPort();
        } catch (ConfigurationException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return dataDir + File.separatorChar + getSubFolder();
  }

  private String dictionaryDir = "dictionary";

  public String getDictionaryDir(boolean flagIsMaster) {
    return getDataDirByInstance(flagIsMaster) + File.separatorChar + dictionaryDir;
  }

  @Property(name = "maxDictionaryWriteBatchSize",
      description = "The number of dictionary entries that are stored before writing them to the database as an atomic write operation.")
  private int maxDictionaryWriteBatchSize = RocksDBDictionary.DEFAULT_MAX_BATCH_SIZE;

  public int getMaxDictionaryWriteBatchSize() {
    return maxDictionaryWriteBatchSize;
  }

  public void setMaxDictionaryWriteBatchSize(int maxDictionaryWriteBatchSize) {
    this.maxDictionaryWriteBatchSize = maxDictionaryWriteBatchSize;
  }

  private String statisticsDir = "statistics";

  public String getStatisticsDir(boolean flagIsMaster) {
    return getDataDirByInstance(flagIsMaster) + File.separatorChar + statisticsDir;
  }

  @Property(name = "tripleStoreStorageType",
      description = "Defines how the triple store is persisted:"
          + "\nMEMORY = triples are only stored in memory"
          + "\nMEMORY_MAPPED_FILE = triples are stored as a file located in dictionaryDir which is mapped to memory. In Linux no additional caching is required."
          + "\nRANDOM_ACCESS_FILE = triples are is stored as a file located in dictionaryDir. Each dictionary lookup will result in a file access.")
  private MapDBStorageOptions tripleStoreStorageType = MapDBStorageOptions.MEMORY_MAPPED_FILE;

  public MapDBStorageOptions getTripleStoreStorageType() {
    return tripleStoreStorageType;
  }

  public void setTripleStoreStorageType(MapDBStorageOptions tripleStoreStorageType) {
    this.tripleStoreStorageType = tripleStoreStorageType;
  }

  private String tripleStoreDir = "tripleStore";

  public String getTripleStoreDir(boolean flagIsMaster) {
    return getDataDirByInstance(flagIsMaster) + File.separatorChar + tripleStoreDir;
  }

  // @Property(name = "enableTransactionsForTripleStore", description = "If
  // set
  // to true, transactions are used. Transactions are only required if
  // processing updates in future work.")
  private boolean useTransactionsForTripleStore = false;

  public boolean useTransactionsForTripleStore() {
    return useTransactionsForTripleStore;
  }

  public void setUseTransactionsForTripleStore(boolean useTransactionsForTripleStore) {
    this.useTransactionsForTripleStore = useTransactionsForTripleStore;
  }

  @Property(name = "enableAsynchronousWritesForTripleStore",
      description = "If set to true, updates are written in a separate thread asynchronously.")
  private boolean isTripleStoreAsynchronouslyWritten = true;

  public boolean isTripleStoreAsynchronouslyWritten() {
    return isTripleStoreAsynchronouslyWritten;
  }

  public void setTripleStoreAsynchronouslyWritten(boolean isTripleStoreAsynchronouslyWritten) {
    this.isTripleStoreAsynchronouslyWritten = isTripleStoreAsynchronouslyWritten;
  }

  @Property(name = "tripleStoreCacheType", description = "Defines how the instance cache works:"
      + "\nNONE = no instances are cached"
      + "\nHASH_TABLE = a cached instance is deleted, if a hash collision occurs"
      + "\nLEAST_RECENTLY_USED = the least recently used instance is deleted, if the cache reaches its maximum size"
      + "\nHARD_REFERENCE = no instance is removed from the cache automatically"
      + "\nSOFT_REFERENCE = instances are removed from the cache by the garbage collector, if no hard reference exists on them and the memory is full"
      + "\nWEAK_REFERENCE = instances are removed from the cache by the garbage collector, as soon as no hard reference exists on them")
  private MapDBCacheOptions tripleStoreCacheType = MapDBCacheOptions.HASH_TABLE;

  public MapDBCacheOptions getTripleStoreCacheType() {
    return tripleStoreCacheType;
  }

  public void setTripleStoreCacheType(MapDBCacheOptions tripleStoreCacheType) {
    this.tripleStoreCacheType = tripleStoreCacheType;
  }

  @Property(name = "sizeOfMappingRecycleCache",
      description = "In order to prevent a frequent garbage collection, Mapping objects are recycled."
          + " This option defines how many Mapping objects should be cached for reuse.")
  private int sizeOfMappingRecycleCache = 100_000;

  public int getSizeOfMappingRecycleCache() {
    return sizeOfMappingRecycleCache;
  }

  public void setSizeOfMappingRecycleCache(int sizeOfMappingRecycleCache) {
    this.sizeOfMappingRecycleCache = sizeOfMappingRecycleCache;
  }

  @Property(name = "unbalanceThresholdForWorkerThreads",
      description = "This property defines how much the current workloads of the different WorkerThreads may differ, before the work is rebalanced.")
  private double unbalanceThresholdForWorkerThreads = 0.1;

  public double getUnbalanceThresholdForWorkerThreads() {
    return unbalanceThresholdForWorkerThreads;
  }

  public void setUnbalanceThresholdForWorkerThreads(double unbalanceThresholdForWorkerThreads) {
    this.unbalanceThresholdForWorkerThreads = unbalanceThresholdForWorkerThreads;
  }

  @Property(name = "mappingBundleSize",
      description = "Before mappings are sent to another computer, they are bundled into one message. This number defines how many mappings are bundeled.")
  private int mappingBundleSize = 100;

  public int getMappingBundleSize() {
    return mappingBundleSize;
  }

  public void setMappingBundleSize(int mappingBundleSize) {
    this.mappingBundleSize = mappingBundleSize;
  }

  @Property(name = "receiverQueueSize",
      description = "Defines how many mappings should be stored in memory for each mapping receiver queue of each query operator")
  private int receiverQueueSize = 1000;

  public int getReceiverQueueSize() {
    return receiverQueueSize;
  }

  public void setReceiverQueueSize(int receiverQueueSize) {
    this.receiverQueueSize = receiverQueueSize;
  }

  @Property(name = "mappingsPerOperationRound",
      description = "Defines the maximum amount of mappings that are emitted by a query operation before the scheduler executes the next operation.")
  private int maxEmittedMappingsPerRound = 100;

  public int getMaxEmittedMappingsPerRound() {
    return maxEmittedMappingsPerRound;
  }

  public void setMaxEmittedMappingsPerRound(int maxEmittedMappingsPerRound) {
    this.maxEmittedMappingsPerRound = maxEmittedMappingsPerRound;
  }

  @Property(name = "joinCacheStorageType", description = "Defines how the join cache is persisted:"
      + "\nMEMORY = triples are only stored in memory"
      + "\nMEMORY_MAPPED_FILE = triples are stored as a file located in dictionaryDir which is mapped to memory. In Linux no additional caching is required."
      + "\nRANDOM_ACCESS_FILE = triples are is stored as a file located in dictionaryDir. Each dictionary lookup will result in a file access.")
  private MapDBStorageOptions joinCacheStorageType = MapDBStorageOptions.MEMORY_MAPPED_FILE;

  public MapDBStorageOptions getJoinCacheStorageType() {
    return joinCacheStorageType;
  }

  public void setJoinCacheStorageType(MapDBStorageOptions joinCacheStorageType) {
    this.joinCacheStorageType = joinCacheStorageType;
  }

  // @Property(name = "enableTransactionsForJoinCache", description = "If
  // set
  // to true, transactions are used. Transactions are only required if
  // processing updates in future work.")
  private boolean useTransactionsForJoinCache = false;

  public boolean useTransactionsForJoinCache() {
    return useTransactionsForJoinCache;
  }

  public void setUseTransactionsForJoinCache(boolean useTransactionsForJoinCache) {
    this.useTransactionsForJoinCache = useTransactionsForJoinCache;
  }

  @Property(name = "enableAsynchronousWritesForJoinCache",
      description = "If set to true, updates are written in a separate thread asynchronously.")
  private boolean isJoinCacheAsynchronouslyWritten = true;

  public boolean isJoinCacheAsynchronouslyWritten() {
    return isJoinCacheAsynchronouslyWritten;
  }

  public void setJoinCacheAsynchronouslyWritten(boolean isJoinCacheAsynchronouslyWritten) {
    this.isJoinCacheAsynchronouslyWritten = isJoinCacheAsynchronouslyWritten;
  }

  @Property(name = "joinCacheType", description = "Defines how the join cache works:"
      + "\nNONE = no instances are cached"
      + "\nHASH_TABLE = a cached instance is deleted, if a hash collision occurs"
      + "\nLEAST_RECENTLY_USED = the least recently used instance is deleted, if the cache reaches its maximum size"
      + "\nHARD_REFERENCE = no instance is removed from the cache automatically"
      + "\nSOFT_REFERENCE = instances are removed from the cache by the garbage collector, if no hard reference exists on them and the memory is full"
      + "\nWEAK_REFERENCE = instances are removed from the cache by the garbage collector, as soon as no hard reference exists on them")
  private MapDBCacheOptions joinCacheType = MapDBCacheOptions.HASH_TABLE;

  public MapDBCacheOptions getJoinCacheType() {
    return joinCacheType;
  }

  public void setJoinCacheType(MapDBCacheOptions joinCacheType) {
    this.joinCacheType = joinCacheType;
  }

  /*
   * StatisticsDB related parameters
   */
  @Property(name = "rowDataLength", description = "Defines how many bytes are used in the statistics database index file for storing either the row data or the reference to the extra file that contains the row data,")
  private int rowDataLength;

  public int getRowDataLength() {
    return rowDataLength;
  }

  public void setRowDataLength(int rowDataLength) {
    this.rowDataLength = rowDataLength;
  }

  @Property(name = "indexCacheSize", description = "Defines how many megabytes will be used to cache index rows in the statistics database.")
  private int indexCacheSize;

  public int getIndexCacheSize() {
    return indexCacheSize;
  }

  public void setIndexCacheSize(int indexCacheSize) {
    this.indexCacheSize = indexCacheSize;
  }

  @Property(name = "extraFilesCacheSize", description = "Defines how many megabytes will be used to cache extra file rows in the statistics database.")
  private int extraCacheSize;

  public int getExtraCacheSize() {
    return extraCacheSize;
  }

  public void setExtraCacheSize(int extraCacheSize) {
    this.extraCacheSize = extraCacheSize;
  }

  @Property(name = "recyclerCapacity", description = "Defines how many blocks can be hold in memory to reuse for storing rows of the statistics files. By default, a block is slightly less than 4096 bytes large.")
  private int recyclerCapacity;

  public int getRecyclerCapacity() {
    return recyclerCapacity;
  }

  public void setRecyclerCapacity(int recyclerCapacity) {
    this.recyclerCapacity = recyclerCapacity;
  }

  @Property(name = "blockSize", description = "Defines the size of a block in bytes (default: 4096), that will be used as the minimum data size. This means, that upon requesting reading of a row, the whole block with the here given size will be loaded into memory from the hard disk.")
  private int blockSize = 4096;

  public int getBlockSize() {
    return blockSize;
  }

  public void setBlockSize(int blockSize) {
    this.blockSize = blockSize;
  }

  @Property(name = "maxOpenFiles", description = "Defines how many files the statistics database is allowed to open.")
  private int maxOpenFiles = 1000;

  public int getMaxOpenFiles() {
    return maxOpenFiles;
  }

  public void setMaxOpenFiles(int maxOpenFiles) {
    this.maxOpenFiles = maxOpenFiles;
  }


  /*
   * serialization specific code
   */

  private ConfigurationSerializer serializer;

  private ConfigurationDeserializer deserializer;

  @Override
  public ConfigurableSerializer getSerializer() {
    if (serializer == null) {
      serializer = new ConfigurationSerializer();
    }
    return serializer;
  }

  @Override
  public ConfigurableDeserializer getDeserializer() {
    if (deserializer == null) {
      deserializer = new ConfigurationDeserializer();
    }
    return deserializer;
  }
}
