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
package de.uni_koblenz.west.koral.slave.triple_store;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.query.Mapping;
import de.uni_koblenz.west.koral.common.query.MappingRecycleCache;
import de.uni_koblenz.west.koral.common.query.TriplePattern;

/**
 * Provides access to the local triple store. I.e., methods to store all triples of a graph file and
 * methods to lookup triples again.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class TripleStoreAccessor implements Closeable, AutoCloseable {

  private final Logger logger;

  private final TripleStore tripleStore;
  
  private long size;

  public TripleStoreAccessor(Configuration conf, Logger logger) {
    this.logger = logger;
    if (conf.getTripleStoreStorageType() == MapDBStorageOptions.MEMORY) {
      tripleStore = new de.uni_koblenz.west.koral.slave.triple_store.impl.TripleStore(
          conf.getTripleStoreStorageType(), conf.getTripleStoreDir(false),
          conf.useTransactionsForTripleStore(), conf.isTripleStoreAsynchronouslyWritten(),
          conf.getTripleStoreCacheType());
    } else {
      tripleStore = new de.uni_koblenz.west.koral.slave.triple_store.impl.TripleStore(
          conf.getTripleStoreDir(false));
    }
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
      tripleStore.flush();
      size = alreadyLoadedTriples;
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

  public long size() {
	  return size;
  }
  
  public void clear() {
    tripleStore.clear();
  }

  @Override
  public void close() {
    tripleStore.close();
  }

}
