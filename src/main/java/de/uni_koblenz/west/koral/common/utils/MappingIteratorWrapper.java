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
package de.uni_koblenz.west.koral.common.utils;

import de.uni_koblenz.west.koral.common.query.Mapping;
import de.uni_koblenz.west.koral.common.query.MappingRecycleCache;

import java.util.Iterator;

public class MappingIteratorWrapper implements Iterable<Mapping>, Iterator<Mapping> {

  private final Iterator<byte[]> iter;

  private final MappingRecycleCache recycleCache;

  public MappingIteratorWrapper(Iterator<byte[]> iter, MappingRecycleCache recycleCache) {
    this.recycleCache = recycleCache;
    this.iter = iter;
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public Mapping next() {
    byte[] next = iter.next();
    return recycleCache.createMapping(next, 0, next.length);
  }

  @Override
  public Iterator<Mapping> iterator() {
    return this;
  }

}
