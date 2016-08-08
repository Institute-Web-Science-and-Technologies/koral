package de.uni_koblenz.west.koral.slave.triple_store;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.common.query.Mapping;
import de.uni_koblenz.west.koral.common.query.MappingRecycleCache;
import de.uni_koblenz.west.koral.common.query.TriplePattern;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Provides access to the local triple store. I.e., methods to store all triples
 * of a graph file and methods to lookup triples again.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class TripleStoreAccessor implements Closeable, AutoCloseable {

  private final Logger logger;

  private final TripleStore tripleStore;

  public TripleStoreAccessor(Configuration conf, Logger logger) {
    this.logger = logger;
    tripleStore = new de.uni_koblenz.west.koral.slave.triple_store.impl.TripleStore(
            conf.getTripleStoreStorageType(), conf.getTripleStoreDir(),
            conf.useTransactionsForTripleStore(), conf.isTripleStoreAsynchronouslyWritten(),
            conf.getTripleStoreCacheType());
  }

  public void storeTriples(File file) {
    try (EncodedFileInputStream in = new EncodedFileInputStream(EncodingFileFormat.EEE, file);) {
      long alreadyLoadedTriples = 0;
      for (Statement statement : in) {
        tripleStore.storeTriple(statement.getSubjectAsLong(), statement.getPropertyAsLong(),
                statement.getObjectAsLong(), statement.getContainment());
        alreadyLoadedTriples++;
        if ((logger != null) && ((alreadyLoadedTriples % 10000) == 0)) {
          logger.finer("loaded " + alreadyLoadedTriples + " triples");
        }
      }
      if (logger != null) {
        logger.finer("finished loading of " + alreadyLoadedTriples + " triples from file "
                + file.getAbsolutePath());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Iterable<Mapping> lookup(MappingRecycleCache cache, TriplePattern triplePattern) {
    return tripleStore.lookup(cache, triplePattern);
  }

  public void clear() {
    tripleStore.clear();
  }

  @Override
  public void close() {
    tripleStore.close();
  }

}
