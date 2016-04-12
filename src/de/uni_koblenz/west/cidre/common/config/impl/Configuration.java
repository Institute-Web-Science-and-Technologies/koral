package de.uni_koblenz.west.cidre.common.config.impl;

import de.uni_koblenz.west.cidre.common.config.Configurable;
import de.uni_koblenz.west.cidre.common.config.ConfigurableDeserializer;
import de.uni_koblenz.west.cidre.common.config.ConfigurableSerializer;
import de.uni_koblenz.west.cidre.common.config.Property;
import de.uni_koblenz.west.cidre.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.cidre.common.mapDB.MapDBDataStructureOptions;
import de.uni_koblenz.west.cidre.common.mapDB.MapDBStorageOptions;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * Contains all configuration options for CIDRE. Options that are serialized in
 * the configuration file are annotated with {@link Property}.
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

  @Property(name = "master", description = "The ip and port of the master server, e.g., 192.168.0.1:4710. If no port is specified, the default port "
          + Configuration.DEFAULT_PORT + " is used.")
  private String masterIP;

  private String masterPort;

  public String[] getMaster() {
    return new String[] { masterIP, masterPort };
  }

  public void setMaster(String masterIP) {
    setMaster(masterIP, Configuration.DEFAULT_PORT);
  }

  public void setMaster(String masterIP, String masterPort) {
    this.masterIP = masterIP;
    this.masterPort = masterPort;
  }

  @Property(name = "ftpServer", description = "The external ip and the internal and external port of the FTP server started at the master server, e.g., 192.168.0.1:2121."
          + " If no port is specified, the default port " + Configuration.DEFAULT_FTP_PORT
          + " is used."
          + " The FTP server is used to upload the graph files from the client to the master and the graph chunks from the master to the slaves."
          + " The FTP server runs only during the filetransfer.")
  private String ftpServerIP;

  private String ftpServerPort;

  public String[] getFTPServer() {
    return new String[] { ftpServerIP, ftpServerPort };
  }

  public void setFTPServer(String ftpServerIP) {
    setMaster(ftpServerIP, Configuration.DEFAULT_FTP_PORT);
  }

  public void setFTPServer(String ftpServerIP, String ftpServerPort) {
    this.ftpServerIP = ftpServerIP;
    this.ftpServerPort = ftpServerPort;
  }

  @Property(name = "slaves", description = "The comma separated list of ips and ports of the different slaves, e.g., 192.168.0.2:4712,192.168.0.3,192.168.0.4:4777. If no port is specified, the default port "
          + Configuration.DEFAULT_PORT + " is used.")
  private List<String> slaveIPs;

  private List<String> slavePorts;

  public String[] getSlave(int index) {
    return new String[] { slaveIPs.get(index), slavePorts.get(index) };
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

  @Property(name = "clientConnection", description = "The ip and port to which clients can connect, e.g., 192.168.0.1:4711. If no port is specified, the default port "
          + Configuration.DEFAULT_CLIENT_PORT + " is used.")
  private String clientIP;

  private String clientPort;

  public String[] getClient() {
    return new String[] { clientIP, clientPort };
  }

  public void setClient(String clientIP) {
    setClient(clientIP, Configuration.DEFAULT_CLIENT_PORT);
  }

  public void setClient(String clientIP, String clientPort) {
    this.clientIP = clientIP;
    this.clientPort = clientPort;
  }

  public static final long CLIENT_CONNECTION_TIMEOUT = 10000;

  public static final long CLIENT_KEEP_ALIVE_INTERVAL = 3000;

  @Property(name = "clientConnectionTimeout", description = "The number of milliseconds the master waits for messages from the client before closing the connection."
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

  @Property(name = "logLevel", description = "Sets the logging level to one of: OFF, SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST, ALL")
  private Level loglevel = Level.INFO;

  @Property(name = "loggingDirectory", description = "Defines an existing directory in which the log files are written.")
  private String logDirectory = ".";

  public Level getLoglevel() {
    return loglevel;
  }

  public void setLoglevel(Level loglevel) {
    this.loglevel = loglevel;
  }

  public String getLogDirectory() {
    return logDirectory;
  }

  public void setLogDirectory(String logDirectory) {
    this.logDirectory = logDirectory;
  }

  @Property(name = "tmpDir", description = "Defines the directory where intermediate data is stored. Default directory (i.e., if not set) is the temporary directory of the operating system.")
  private String tmpDir = System.getProperty("java.io.tmpdir");

  public String getTmpDir() {
    return tmpDir;
  }

  public void setTmpDir(String tmpDir) {
    this.tmpDir = tmpDir;
  }

  @Property(name = "dictionaryStorageType", description = "Defines how the dictionary is persisted:"
          + "\nMEMORY = dictionary is only stored in memory"
          + "\nMEMORY_MAPPED_FILE = dictionary is stored as a file located in dictionaryDir which is mapped to memory. In Linux no additional caching is required."
          + "\nRANDOM_ACCESS_FILE = dictionary is stored as a file located in dictionaryDir. Each dictionary lookup will result in a file access.")
  private MapDBStorageOptions dictionaryStorageType = MapDBStorageOptions.MEMORY_MAPPED_FILE;

  public MapDBStorageOptions getDictionaryStorageType() {
    return dictionaryStorageType;
  }

  public void setDictionaryStorageType(MapDBStorageOptions dictionaryStorageType) {
    this.dictionaryStorageType = dictionaryStorageType;
  }

  @Property(name = "dictionaryDataStructure", description = "Defines the data structure used for storing the dictionary:"
          + "\nHASH_TREE_MAP = fast single thread access, slow concurrent access, memory efficient"
          + "\nB_TREE_MAP = slower single thread access, faster concurrent access, costs more memory")
  private MapDBDataStructureOptions dictionaryDataStructure = MapDBDataStructureOptions.HASH_TREE_MAP;

  public MapDBDataStructureOptions getDictionaryDataStructure() {
    return dictionaryDataStructure;
  }

  public void setDictionaryDataStructure(MapDBDataStructureOptions dictionaryDataStructure) {
    this.dictionaryDataStructure = dictionaryDataStructure;
  }

  @Property(name = "dictionaryDir", description = "Defines a non-existing directory where the dictionary is stored.")
  private String dictionaryDir = "." + File.separatorChar + "dictionary";

  public String getDictionaryDir() {
    return dictionaryDir;
  }

  public void setDictionaryDir(String dictionaryDir) {
    this.dictionaryDir = dictionaryDir;
  }

  // @Property(name = "enableTransactionsForDictionary", description = "If set
  // to true, transactions are used. Transactions are only required if
  // processing updates in future work.")
  private boolean useTransactionsForDictionary = false;

  public boolean useTransactionsForDictionary() {
    return useTransactionsForDictionary;
  }

  public void setUseTransactionsForDictionary(boolean useTransactions) {
    useTransactionsForDictionary = useTransactions;
  }

  @Property(name = "enableAsynchronousWritesForDictionary", description = "If set to true, updates are written in a separate thread asynchronously.")
  private boolean isDictionaryAsynchronouslyWritten = true;

  public boolean isDictionaryAsynchronouslyWritten() {
    return isDictionaryAsynchronouslyWritten;
  }

  public void setDictionaryAsynchronouslyWritten(boolean isAsynchronousWritten) {
    isDictionaryAsynchronouslyWritten = isAsynchronousWritten;
  }

  @Property(name = "dictionaryCacheType", description = "Defines how the instance cache works:"
          + "\nNONE = no instances are cached"
          + "\nHASH_TABLE = a cached instance is deleted, if a hash collision occurs"
          + "\nLEAST_RECENTLY_USED = the least recently used instance is deleted, if the cache reaches its maximum size"
          + "\nHARD_REFERENCE = no instance is removed from the cache automatically"
          + "\nSOFT_REFERENCE = instances are removed from the cache by the garbage collector, if no hard reference exists on them and the memory is full"
          + "\nWEAK_REFERENCE = instances are removed from the cache by the garbage collector, as soon as no hard reference exists on them")
  private MapDBCacheOptions dictionaryCacheType = MapDBCacheOptions.HASH_TABLE;

  public MapDBCacheOptions getDictionaryCacheType() {
    return dictionaryCacheType;
  }

  public void setDictionaryCacheType(MapDBCacheOptions dictionaryCacheType) {
    this.dictionaryCacheType = dictionaryCacheType;
  }

  @Property(name = "statisticsStorageType", description = "Defines how the statistics tables are persisted:"
          + "\nMEMORY = tables are only stored in memory"
          + "\nMEMORY_MAPPED_FILE = tables are stored as a file located in dictionaryDir which is mapped to memory. In Linux no additional caching is required."
          + "\nRANDOM_ACCESS_FILE = tables are is stored as a file located in dictionaryDir. Each dictionary lookup will result in a file access.")
  private MapDBStorageOptions statisticsStorageType = MapDBStorageOptions.MEMORY_MAPPED_FILE;

  public MapDBStorageOptions getStatisticsStorageType() {
    return statisticsStorageType;
  }

  public void setStatisticsStorageType(MapDBStorageOptions statisticsStorageType) {
    this.statisticsStorageType = statisticsStorageType;
  }

  @Property(name = "statisticsDataStructure", description = "Defines the data structure used for storing the statiscts tables:"
          + "\nHASH_TREE_MAP = fast single thread access, slow concurrent access, memory efficient"
          + "\nB_TREE_MAP = slower single thread access, faster concurrent access, costs more memory")
  private MapDBDataStructureOptions statisticsDataStructure = MapDBDataStructureOptions.HASH_TREE_MAP;

  public MapDBDataStructureOptions getStatisticsDataStructure() {
    return statisticsDataStructure;
  }

  public void setStatisticsDataStructure(MapDBDataStructureOptions statisticsDataStructure) {
    this.statisticsDataStructure = statisticsDataStructure;
  }

  @Property(name = "statisticsDir", description = "Defines a non-existing directory where the statistics are stored.")
  private String statisticsDir = "." + File.separatorChar + "statistics";

  public String getStatisticsDir() {
    return statisticsDir;
  }

  public void setStatisticsDir(String statisticsDir) {
    this.statisticsDir = statisticsDir;
  }

  // @Property(name = "enableTransactionsForStatistics", description = "If set
  // to true, transactions are used. Transactions are only required if
  // processing updates in future work.")
  private boolean useTransactionsForStatistics = false;

  public boolean useTransactionsForStatistics() {
    return useTransactionsForStatistics;
  }

  public void setUseTransactionsForStatistics(boolean useTransactionsForStatistics) {
    this.useTransactionsForStatistics = useTransactionsForStatistics;
  }

  @Property(name = "enableAsynchronousWritesForStatistics", description = "If set to true, updates are written in a separate thread asynchronously.")
  private boolean areStatisticsAsynchronouslyWritten = true;

  public boolean areStatisticsAsynchronouslyWritten() {
    return areStatisticsAsynchronouslyWritten;
  }

  public void setStatisticsAsynchronouslyWritten(boolean isAsynchronousWritten) {
    areStatisticsAsynchronouslyWritten = isAsynchronousWritten;
  }

  @Property(name = "statisticsCacheType", description = "Defines how the instance cache works:"
          + "\nNONE = no instances are cached"
          + "\nHASH_TABLE = a cached instance is deleted, if a hash collision occurs"
          + "\nLEAST_RECENTLY_USED = the least recently used instance is deleted, if the cache reaches its maximum size"
          + "\nHARD_REFERENCE = no instance is removed from the cache automatically"
          + "\nSOFT_REFERENCE = instances are removed from the cache by the garbage collector, if no hard reference exists on them and the memory is full"
          + "\nWEAK_REFERENCE = instances are removed from the cache by the garbage collector, as soon as no hard reference exists on them")
  private MapDBCacheOptions statisticsCacheType = MapDBCacheOptions.HASH_TABLE;

  public MapDBCacheOptions getStatisticsCacheType() {
    return statisticsCacheType;
  }

  public void setStatisticsCacheType(MapDBCacheOptions statisticsCacheType) {
    this.statisticsCacheType = statisticsCacheType;
  }

  @Property(name = "tripleStoreStorageType", description = "Defines how the triple store is persisted:"
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

  @Property(name = "tripleStoreDir", description = "Defines a non-existing directory where the triples are stored.")
  private String tripleStoreDir = "." + File.separatorChar + "tripleStore";

  public String getTripleStoreDir() {
    return tripleStoreDir;
  }

  public void setTripleStoreDir(String tripleStoreDir) {
    this.tripleStoreDir = tripleStoreDir;
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

  @Property(name = "enableAsynchronousWritesForTripleStore", description = "If set to true, updates are written in a separate thread asynchronously.")
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

  @Property(name = "sizeOfMappingRecycleCache", description = "In order to prevent a frequent garbage collection, Mapping objects are recycled."
          + " This option defines how many Mapping objects should be cached for reuse.")
  private int sizeOfMappingRecycleCache = 100_000;

  public int getSizeOfMappingRecycleCache() {
    return sizeOfMappingRecycleCache;
  }

  public void setSizeOfMappingRecycleCache(int sizeOfMappingRecycleCache) {
    this.sizeOfMappingRecycleCache = sizeOfMappingRecycleCache;
  }

  @Property(name = "unbalanceThresholdForWorkerThreads", description = "This property defines how much the current workloads of the different WorkerThreads may differ, before the work is rebalanced.")
  private double unbalanceThresholdForWorkerThreads = 0.1;

  public double getUnbalanceThresholdForWorkerThreads() {
    return unbalanceThresholdForWorkerThreads;
  }

  public void setUnbalanceThresholdForWorkerThreads(double unbalanceThresholdForWorkerThreads) {
    this.unbalanceThresholdForWorkerThreads = unbalanceThresholdForWorkerThreads;
  }

  @Property(name = "mappingBundleSize", description = "Before mappings are sent to another computer, they are bundled into one message. This number defines how many mappings are bundeled.")
  private int mappingBundleSize = 100;

  public int getMappingBundleSize() {
    return mappingBundleSize;
  }

  public void setMappingBundleSize(int mappingBundleSize) {
    this.mappingBundleSize = mappingBundleSize;
  }

  @Property(name = "receiverQueueSize", description = "Defines how many mappings should be stored in memory for each mapping receiver queue of each query operator")
  private int receiverQueueSize = 1000;

  public int getReceiverQueueSize() {
    return receiverQueueSize;
  }

  public void setReceiverQueueSize(int receiverQueueSize) {
    this.receiverQueueSize = receiverQueueSize;
  }

  @Property(name = "mappingsPerOperationRound", description = "Defines the maximum amount of mappings that are emitted by a query operation before the scheduler executes the next operation.")
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

  @Property(name = "enableAsynchronousWritesForJoinCache", description = "If set to true, updates are written in a separate thread asynchronously.")
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
