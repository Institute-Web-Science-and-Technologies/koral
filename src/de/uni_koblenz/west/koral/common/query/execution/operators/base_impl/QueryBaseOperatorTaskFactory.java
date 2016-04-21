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
