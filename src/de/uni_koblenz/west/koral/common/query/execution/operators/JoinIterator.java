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
package de.uni_koblenz.west.koral.common.query.execution.operators;

import de.uni_koblenz.west.koral.common.query.Mapping;
import de.uni_koblenz.west.koral.common.query.MappingRecycleCache;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class JoinIterator implements Iterator<Mapping> {

  private final MappingRecycleCache recycleCache;

  private final long[] resultVars;

  private final long[] joinVars;

  private final Mapping joiningMapping;

  private final long[] varsOfJoiningMapping;

  private final Iterator<Mapping> joinCandidates;

  private final long[] varsOfJoinCandidates;

  private Mapping next;

  private long numberOfComparisons = 0;

  public JoinIterator(MappingRecycleCache recycleCache, long[] resultVars, long[] joinVars,
          Mapping joiningMapping, long[] varsOfJoiningMapping, Iterator<Mapping> joinCandidates,
          long[] varsOfJoinCandidates) {
    super();
    this.recycleCache = recycleCache;
    this.resultVars = resultVars;
    this.joinVars = joinVars;
    this.joiningMapping = joiningMapping;
    this.varsOfJoiningMapping = varsOfJoiningMapping;
    this.joinCandidates = joinCandidates;
    this.varsOfJoinCandidates = varsOfJoinCandidates;
    next = getNext();
  }

  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public Mapping next() {
    if (next == null) {
      throw new NoSuchElementException();
    }
    Mapping n = next;
    next = getNext();
    return n;
  }

  private Mapping getNext() {
    while (joinCandidates.hasNext()) {
      Mapping joinCandidate = joinCandidates.next();
      if (areJoinVarValuesEqual(joiningMapping, varsOfJoiningMapping, joinCandidate,
              varsOfJoinCandidates)) {
        return recycleCache.mergeMappings(resultVars, joiningMapping, varsOfJoiningMapping,
                joinCandidate, varsOfJoinCandidates);
      }
    }
    return null;
  }

  private boolean areJoinVarValuesEqual(Mapping mapping1, long[] vars1, Mapping mapping2,
          long[] vars2) {
    numberOfComparisons++;
    for (long var : joinVars) {
      if (mapping1.getValue(var, vars1) != mapping2.getValue(var, vars2)) {
        return false;
      }
    }
    return true;
  }

  public Mapping getJoiningMapping() {
    return joiningMapping;
  }

  public long getNumberOfComparisons() {
    return numberOfComparisons;
  }

}
