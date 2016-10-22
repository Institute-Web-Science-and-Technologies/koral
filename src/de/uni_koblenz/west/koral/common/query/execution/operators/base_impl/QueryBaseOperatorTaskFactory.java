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

import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.query.TriplePattern;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorTaskFactoryBase;
import de.uni_koblenz.west.koral.common.query.execution.operators.SliceOperator;
import de.uni_koblenz.west.koral.slave.triple_store.TripleStoreAccessor;

import java.io.File;

public class QueryBaseOperatorTaskFactory extends QueryOperatorTaskFactoryBase {

  public QueryBaseOperatorTaskFactory(long coordinatorId, int numberOfSlaves, int cacheSize,
          File cacheDirectory) {
    super(coordinatorId, numberOfSlaves, cacheSize, cacheDirectory);
  }

  public QueryBaseOperatorTaskFactory(QueryOperatorTaskFactoryBase taskFactory) {
    super(taskFactory);
  }

  @Override
  public QueryOperatorTask createTriplePatternMatch(long taskId, int emittedMappingsPerRound,
          TriplePattern pattern, TripleStoreAccessor tripleStore) {
    return new TriplePatternMatchBaseOperator(taskId, coordinatorId, numberOfSlaves, cacheSize,
            cacheDirectory, pattern, emittedMappingsPerRound, tripleStore);
  }

  @Override
  public QueryOperatorTask createTriplePatternJoin(long taskId, int emittedMappingsPerRound,
          QueryOperatorTask leftChild, QueryOperatorTask rightChild,
          MapDBStorageOptions storageType, boolean useTransactions, boolean writeAsynchronously,
          MapDBCacheOptions cacheType) {
    return new TriplePatternJoinBaseOperator(taskId, coordinatorId, numberOfSlaves, cacheSize,
            cacheDirectory, emittedMappingsPerRound, leftChild, rightChild, storageType,
            useTransactions, writeAsynchronously, cacheType);
  }

  @Override
  public QueryOperatorTask createProjection(long taskId, int emittedMappingsPerRound,
          long[] resultVars, QueryOperatorTask subOperation) {
    return new ProjectionBaseOperator(taskId, coordinatorId, numberOfSlaves, cacheSize,
            cacheDirectory, emittedMappingsPerRound, resultVars, subOperation);
  }

  @Override
  public QueryOperatorTask createSlice(long taskId, int emittedMappingsPerRound,
          QueryOperatorTask subOperation, long offset, long length) {
    return new SliceOperator(taskId, coordinatorId, numberOfSlaves, cacheSize, cacheDirectory,
            emittedMappingsPerRound, subOperation, offset, length);
  }

}
