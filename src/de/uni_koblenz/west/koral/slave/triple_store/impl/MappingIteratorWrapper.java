/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License
 * along with Koral.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
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

  public void close() {
    if (iter instanceof RocksIteratorKeyWrapper) {
      ((RocksIteratorKeyWrapper) iter).close();
    }
  }

}
