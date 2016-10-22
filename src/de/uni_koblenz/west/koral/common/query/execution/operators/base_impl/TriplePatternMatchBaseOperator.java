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
package de.uni_koblenz.west.koral.common.query.execution.operators.base_impl;

import de.uni_koblenz.west.koral.common.query.Mapping;
import de.uni_koblenz.west.koral.common.query.TriplePattern;
import de.uni_koblenz.west.koral.common.query.execution.operators.TriplePatternMatchOperator;
import de.uni_koblenz.west.koral.slave.triple_store.TripleStoreAccessor;

import java.io.File;

public class TriplePatternMatchBaseOperator extends TriplePatternMatchOperator {

  public TriplePatternMatchBaseOperator(long id, long coordinatorId, int numberOfSlaves,
          int cacheSize, File cacheDirectory, TriplePattern pattern, int emittedMappingsPerRound,
          TripleStoreAccessor tripleStore) {
    super(id, coordinatorId, numberOfSlaves, cacheSize, cacheDirectory, pattern,
            emittedMappingsPerRound, tripleStore);
  }

  public TriplePatternMatchBaseOperator(short slaveId, int queryId, short taskId,
          long coordinatorId, int numberOfSlaves, int cacheSize, File cacheDirectory,
          TriplePattern pattern, int emittedMappingsPerRound, TripleStoreAccessor tripleStore) {
    super(slaveId, queryId, taskId, coordinatorId, numberOfSlaves, cacheSize, cacheDirectory,
            pattern, emittedMappingsPerRound, tripleStore);
  }

  @Override
  protected void emitMapping(Mapping mapping) {
    if (getParentTask() == null) {
      messageSender.sendQueryMapping(mapping, getID(), getCoordinatorID(), recycleCache);
      numberOfEmittedMappings[0]++;
    } else {
      // send to computer with smallest id
      long parentBaseID = getParentTask().getID() & 0x00_00_FF_FF_FF_FF_FF_FFl;
      messageSender.sendQueryMapping(mapping, getID(), parentBaseID | 0x00_01_00_00_00_00_00_00l,
              recycleCache);
      numberOfEmittedMappings[1]++;
    }
  }

}
