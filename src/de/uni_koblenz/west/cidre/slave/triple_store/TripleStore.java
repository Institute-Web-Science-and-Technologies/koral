package de.uni_koblenz.west.cidre.slave.triple_store;

import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;
import de.uni_koblenz.west.cidre.common.query.TriplePattern;

import java.io.Closeable;

/**
 * Declares all methods required by {@link TripleStoreAccessor} to interact with
 * the local triple store.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface TripleStore extends Closeable, AutoCloseable {

  public void storeTriple(long subject, long property, long object, byte[] containment);

  public Iterable<Mapping> lookup(MappingRecycleCache cache, TriplePattern triplePattern);

  public void clear();

  @Override
  public void close();

}
