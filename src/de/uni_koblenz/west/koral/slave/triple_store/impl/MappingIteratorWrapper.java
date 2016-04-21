package de.uni_koblenz.west.koral.slave.triple_store.impl;

import de.uni_koblenz.west.koral.common.query.Mapping;
import de.uni_koblenz.west.koral.common.query.MappingRecycleCache;
import de.uni_koblenz.west.koral.common.query.TriplePattern;

import java.util.Iterator;

/**
 * Index look ups in the indices of {@link MapDBTripleStore} would result in
 * Iterators over byte array representation of matching triples. This wrapper
 * converts the returned byte arrays into the corresponding {@link Mapping}s.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MappingIteratorWrapper implements Iterable<Mapping>, Iterator<Mapping> {

  private final MappingRecycleCache cache;

  private final TriplePattern pattern;

  private final IndexType indexType;

  private final Iterator<byte[]> iter;

  public MappingIteratorWrapper(MappingRecycleCache cache, TriplePattern pattern,
          IndexType indexType, Iterator<byte[]> iter) {
    this.cache = cache;
    this.pattern = pattern;
    this.indexType = indexType;
    this.iter = iter;
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public Mapping next() {
    byte[] triple = iter.next();
    return cache.createMapping(pattern, indexType, triple);
  }

  @Override
  public Iterator<Mapping> iterator() {
    return this;
  }

}
