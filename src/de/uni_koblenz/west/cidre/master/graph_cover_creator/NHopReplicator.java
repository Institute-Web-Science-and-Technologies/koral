package de.uni_koblenz.west.cidre.master.graph_cover_creator;

import java.io.File;
import java.util.logging.Logger;

public class NHopReplicator {

  private final Logger logger;

  public NHopReplicator(Logger logger) {
    this.logger = logger;
  }

  public File[] createNHopReplication(File[] graphCover, File workingDir) {
    // mapFolder = new File(cacheDirectory.getAbsolutePath() + File.separator
    // + uniqueFileNameSuffix);
    // if (!mapFolder.exists()) {
    // mapFolder.mkdirs();
    // }
    // DBMaker<?> dbmaker = storageType.getDBMaker(mapFolder.getAbsolutePath()
    // + File.separator + uniqueFileNameSuffix);
    // if (!useTransactions) {
    // dbmaker = dbmaker.transactionDisable().closeOnJvmShutdown();
    // }
    // if (writeAsynchronously) {
    // dbmaker = dbmaker.asyncWriteEnable();
    // }
    // dbmaker = cacheType.setCaching(dbmaker);
    // database = dbmaker.make();
    //
    // multiMap = database.createTreeSet(uniqueFileNameSuffix)
    // .comparator(new JoinComparator(variableComparisonOrder))
    // .makeOrGet();
    // TODO implement
    // graphCover[i] might be null
    return null;
  }

}
