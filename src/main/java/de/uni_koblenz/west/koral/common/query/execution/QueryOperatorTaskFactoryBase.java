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
package de.uni_koblenz.west.koral.common.query.execution;

import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.query.TriplePattern;
import de.uni_koblenz.west.koral.slave.triple_store.TripleStoreAccessor;

import java.io.File;

/**
 * Provides base functionality for the creation and deserialization of query
 * operator tasks.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public abstract class QueryOperatorTaskFactoryBase {

  private int nextTaskId;

  protected final long coordinatorId;

  protected final int numberOfSlaves;

  protected final int cacheSize;

  protected final File cacheDirectory;

  public QueryOperatorTaskFactoryBase(long coordinatorId, int numberOfSlaves, int cacheSize,
          File cacheDirectory) {
    nextTaskId = 0;
    this.coordinatorId = coordinatorId;
    this.numberOfSlaves = numberOfSlaves;
    this.cacheSize = cacheSize;
    this.cacheDirectory = cacheDirectory;
  }

  public QueryOperatorTaskFactoryBase(QueryOperatorTaskFactoryBase taskFactory) {
    this(taskFactory.coordinatorId, taskFactory.numberOfSlaves, taskFactory.cacheSize,
            taskFactory.cacheDirectory);
  }

  private short getNextTaskId() {
    if (nextTaskId > ((Short.MAX_VALUE) - Short.MIN_VALUE)) {
      throw new RuntimeException("The maximal number of tasks have already been created.");
    }
    return (short) nextTaskId++;
  }

  private long getNewTaskId(short slaveId, int queryId) {
    return (((((long) slaveId) << Integer.SIZE)
            | (queryId & 0x00_00_00_00_ff_ff_ff_ffl)) << Short.SIZE)
            | (getNextTaskId() & 0x00_00_00_00_00_00_ff_ffl);
  }

  public QueryOperatorTask createTriplePatternMatch(short slaveId, int queryId,
          int emittedMappingsPerRound, TriplePattern pattern, TripleStoreAccessor tripleStore) {
    return createTriplePatternMatch(getNewTaskId(slaveId, queryId), emittedMappingsPerRound,
            pattern, tripleStore);
  }

  public abstract QueryOperatorTask createTriplePatternMatch(long taskId,
          int emittedMappingsPerRound, TriplePattern pattern, TripleStoreAccessor tripleStore);

  public QueryOperatorTask createTriplePatternJoin(short slaveId, int queryId,
          int emittedMappingsPerRound, QueryOperatorTask leftChild, QueryOperatorTask rightChild,
          MapDBStorageOptions storageType, boolean useTransactions, boolean writeAsynchronously,
          MapDBCacheOptions cacheType) {
    return createTriplePatternJoin(getNewTaskId(slaveId, queryId), emittedMappingsPerRound,
            leftChild, rightChild, storageType, useTransactions, writeAsynchronously, cacheType);
  }

  public abstract QueryOperatorTask createTriplePatternJoin(long taskId,
          int emittedMappingsPerRound, QueryOperatorTask leftChild, QueryOperatorTask rightChild,
          MapDBStorageOptions storageType, boolean useTransactions, boolean writeAsynchronously,
          MapDBCacheOptions cacheType);

  public QueryOperatorTask createProjection(short slaveId, int queryId, int emittedMappingsPerRound,
          long[] resultVars, QueryOperatorTask subOperation) {
    return createProjection(getNewTaskId(slaveId, queryId), emittedMappingsPerRound, resultVars,
            subOperation);
  }

  public abstract QueryOperatorTask createProjection(long taskId, int emittedMappingsPerRound,
          long[] resultVars, QueryOperatorTask subOperation);

  public QueryOperatorTask createSlice(short slaveId, int queryId, int emittedMappingsPerRound,
          QueryOperatorTask subOperation, long offset, long length) {
    return createSlice(getNewTaskId(slaveId, queryId), emittedMappingsPerRound, subOperation,
            offset, length);
  }

  public abstract QueryOperatorTask createSlice(long taskId, int emittedMappingsPerRound,
          QueryOperatorTask subOperation, long offset, long length);

}
