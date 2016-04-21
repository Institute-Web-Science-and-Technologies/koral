package de.uni_koblenz.west.koral.common.query.execution.operators.base_impl;

import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.query.Mapping;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.koral.common.query.execution.operators.TriplePatternJoinOperator;

import java.io.File;

public class TriplePatternJoinBaseOperator extends TriplePatternJoinOperator {

  public TriplePatternJoinBaseOperator(long id, long coordinatorId, int numberOfSlaves,
          int cacheSize, File cacheDirectory, int emittedMappingsPerRound,
          QueryOperatorTask leftChild, QueryOperatorTask rightChild,
          MapDBStorageOptions storageType, boolean useTransactions, boolean writeAsynchronously,
          MapDBCacheOptions cacheType) {
    super(id, coordinatorId, numberOfSlaves, cacheSize, cacheDirectory, emittedMappingsPerRound,
            leftChild, rightChild, storageType, useTransactions, writeAsynchronously, cacheType);
  }

  public TriplePatternJoinBaseOperator(short slaveId, int queryId, short taskId, long coordinatorId,
          int numberOfSlaves, int cacheSize, File cacheDirectory, int emittedMappingsPerRound,
          QueryOperatorTask leftChild, QueryOperatorTask rightChild,
          MapDBStorageOptions storageType, boolean useTransactions, boolean writeAsynchronously,
          MapDBCacheOptions cacheType) {
    super(slaveId, queryId, taskId, coordinatorId, numberOfSlaves, cacheSize, cacheDirectory,
            emittedMappingsPerRound, leftChild, rightChild, storageType, useTransactions,
            writeAsynchronously, cacheType);
  }

  @Override
  protected void emitMapping(Mapping mapping) {
    if (getParentTask() == null) {
      messageSender.sendQueryMapping(mapping, getID(), getCoordinatorID(), recycleCache);
    } else {
      // send to computer with smallest id
      long parentBaseID = getParentTask().getID() & 0x00_00_FF_FF_FF_FF_FF_FFl;
      messageSender.sendQueryMapping(mapping, getID(), parentBaseID | 0x00_01_00_00_00_00_00_00l,
              recycleCache);
    }
  }

}
