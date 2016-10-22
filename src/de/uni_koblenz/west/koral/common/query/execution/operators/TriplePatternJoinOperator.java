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

import de.uni_koblenz.west.koral.common.executor.messagePassing.MessageSenderBuffer;
import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.measurement.MeasurementType;
import de.uni_koblenz.west.koral.common.query.Mapping;
import de.uni_koblenz.west.koral.common.query.MappingRecycleCache;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorBase;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorType;
import de.uni_koblenz.west.koral.common.utils.InMemoryJoinMappingCache;
import de.uni_koblenz.west.koral.common.utils.JoinMappingCache;
import de.uni_koblenz.west.koral.common.utils.MapDBJoinMappingCache;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Logger;

/**
 * Performs the join operation of mappings as a hash join.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class TriplePatternJoinOperator extends QueryOperatorBase {

  private long[] resultVars;

  private long[] joinVars;

  private final JoinType joinType;

  private final MapDBStorageOptions storageType;

  private final boolean useTransactions;

  private final boolean writeAsynchronously;

  private final MapDBCacheOptions cacheType;

  private JoinMappingCache leftMappingCache;

  private JoinMappingCache rightMappingCache;

  private JoinIterator iterator;

  /*
   * variables for measurement
   */

  private long numberOfComparisons;

  public TriplePatternJoinOperator(long id, long coordinatorId, int numberOfSlaves, int cacheSize,
          File cacheDirectory, int emittedMappingsPerRound, QueryOperatorTask leftChild,
          QueryOperatorTask rightChild, MapDBStorageOptions storageType, boolean useTransactions,
          boolean writeAsynchronously, MapDBCacheOptions cacheType) {
    super(id, coordinatorId, numberOfSlaves, cacheSize, cacheDirectory, emittedMappingsPerRound);
    addChildTask(leftChild);
    addChildTask(rightChild);
    computeVars(leftChild.getResultVariables(), rightChild.getResultVariables());

    if (joinVars.length > 0) {
      joinType = JoinType.JOIN;
    } else {
      if (leftChild.getResultVariables().length == 0) {
        joinType = JoinType.RIGHT_FORWARD;
      } else if (rightChild.getResultVariables().length == 0) {
        joinType = JoinType.LEFT_FORWARD;
      } else {
        joinType = JoinType.CARTESIAN_PRODUCT;
      }
    }

    this.cacheType = cacheType;
    this.storageType = storageType;
    this.useTransactions = useTransactions;
    this.writeAsynchronously = writeAsynchronously;
  }

  public TriplePatternJoinOperator(short slaveId, int queryId, short taskId, long coordinatorId,
          int numberOfSlaves, int cacheSize, File cacheDirectory, int emittedMappingsPerRound,
          QueryOperatorTask leftChild, QueryOperatorTask rightChild,
          MapDBStorageOptions storageType, boolean useTransactions, boolean writeAsynchronously,
          MapDBCacheOptions cacheType) {
    super(slaveId, queryId, taskId, coordinatorId, numberOfSlaves, cacheSize, cacheDirectory,
            emittedMappingsPerRound);
    addChildTask(leftChild);
    addChildTask(rightChild);
    computeVars(leftChild.getResultVariables(), rightChild.getResultVariables());

    if (joinVars.length > 0) {
      joinType = JoinType.JOIN;
    } else {
      if (leftChild.getResultVariables().length == 0) {
        joinType = JoinType.RIGHT_FORWARD;
      } else if (rightChild.getResultVariables().length == 0) {
        joinType = JoinType.LEFT_FORWARD;
      } else {
        joinType = JoinType.CARTESIAN_PRODUCT;
      }
    }

    this.cacheType = cacheType;
    this.storageType = storageType;
    this.useTransactions = useTransactions;
    this.writeAsynchronously = writeAsynchronously;
  }

  @Override
  public void setUp(MessageSenderBuffer messageSender, MappingRecycleCache recycleCache,
          Logger logger, MeasurementCollector measurementCollector) {
    super.setUp(messageSender, recycleCache, logger, measurementCollector);
    long[] leftVars = ((QueryOperatorTask) getChildTask(0)).getResultVariables();
    long[] rightVars = ((QueryOperatorTask) getChildTask(1)).getResultVariables();
    if (storageType == MapDBStorageOptions.MEMORY) {
      leftMappingCache = new InMemoryJoinMappingCache(leftVars, createComparisonOrder(leftVars),
              joinVars.length);
      rightMappingCache = new InMemoryJoinMappingCache(rightVars, createComparisonOrder(rightVars),
              joinVars.length);
    } else {
      leftMappingCache = new MapDBJoinMappingCache(storageType, useTransactions,
              writeAsynchronously, cacheType, getCacheDirectory(), recycleCache,
              getClass().getSimpleName() + getID() + "_leftChild_", leftVars,
              createComparisonOrder(leftVars), joinVars.length);
      rightMappingCache = new MapDBJoinMappingCache(storageType, useTransactions,
              writeAsynchronously, cacheType, getCacheDirectory(), recycleCache,
              getClass().getSimpleName() + getID() + "_rightChild_", rightVars,
              createComparisonOrder(rightVars), joinVars.length);
    }
  }

  private int[] createComparisonOrder(long[] vars) {
    int[] ordering = new int[vars.length];
    int nextIndex = 0;
    for (long var : joinVars) {
      ordering[nextIndex] = getIndexOfVar(var, vars);
      nextIndex++;
    }
    for (int i = 0; i < vars.length; i++) {
      if (getIndexOfVar(vars[i], joinVars) == -1) {
        ordering[nextIndex] = i;
        nextIndex++;
      }
    }
    return ordering;
  }

  private int getIndexOfVar(long var, long[] vars) {
    for (int i = 0; i < vars.length; i++) {
      if (vars[i] == var) {
        return i;
      }
    }
    return -1;
  }

  private void computeVars(long[] leftVars, long[] rightVars) {
    long[] leftResultVars = ((QueryOperatorBase) getChildTask(0)).getResultVariables();
    long[] rightResultVars = ((QueryOperatorBase) getChildTask(1)).getResultVariables();
    if (leftResultVars.length == 0) {
      joinVars = new long[0];
      resultVars = rightResultVars;
    } else if (rightResultVars.length == 0) {
      joinVars = new long[0];
      resultVars = leftResultVars;
    } else {
      long[] allVars = new long[leftVars.length + rightVars.length];
      System.arraycopy(leftVars, 0, allVars, 0, leftVars.length);
      System.arraycopy(rightVars, 0, allVars, leftVars.length, rightVars.length);
      Arrays.sort(allVars);
      // count occurrences of different variable types
      int numberOfJoinVars = 0;
      int numberOfResultVars = 0;
      for (int i = 0; i < allVars.length; i++) {
        if ((i > 0) && (allVars[i - 1] == allVars[i])) {
          // each variable occurs at most two times
          numberOfJoinVars++;
        } else {
          numberOfResultVars++;
        }
      }
      // assign variables to arrays
      resultVars = new long[numberOfResultVars];
      joinVars = new long[numberOfJoinVars];
      int nextJoinVarIndex = 0;
      for (int i = 0; i < allVars.length; i++) {
        if ((i > 0) && (allVars[i - 1] == allVars[i])) {
          // each variable occurs at most two times
          joinVars[nextJoinVarIndex] = allVars[i];
          nextJoinVarIndex++;
        } else {
          resultVars[i - nextJoinVarIndex] = allVars[i];
        }
      }
    }
  }

  @Override
  public long computeEstimatedLoad(GraphStatistics statistics, int slave, boolean setLoads) {
    long joinSize = computeTotalEstimatedLoad(statistics) / statistics.getNumberOfChunks();
    if (setLoads) {
      ((QueryOperatorBase) getChildTask(0)).computeEstimatedLoad(statistics, slave, setLoads);
      ((QueryOperatorBase) getChildTask(1)).computeEstimatedLoad(statistics, slave, setLoads);
      setEstimatedWorkLoad(joinSize);
    }
    return joinSize;
  }

  @Override
  public long computeTotalEstimatedLoad(GraphStatistics statistics) {
    QueryOperatorBase leftChild = (QueryOperatorBase) getChildTask(0);
    long leftLoad = leftChild.computeTotalEstimatedLoad(statistics);
    if (leftLoad == 0) {
      return 0;
    }
    QueryOperatorBase rightChild = (QueryOperatorBase) getChildTask(1);
    long rightLoad = rightChild.computeTotalEstimatedLoad(statistics);
    if (rightLoad == 0) {
      return 0;
    }
    return leftLoad * rightLoad;
  }

  @Override
  public long[] getResultVariables() {
    return resultVars;
  }

  @Override
  public long getFirstJoinVar() {
    return joinVars.length == 0 ? -1 : joinVars[0];
  }

  @Override
  public long getCurrentTaskLoad() {
    long leftSize = getSizeOfInputQueue(0) + leftMappingCache.size();
    long rightSize = getSizeOfInputQueue(1) + rightMappingCache.size();
    if (leftSize == 0) {
      return rightSize;
    } else if (rightSize == 0) {
      return leftSize;
    } else {
      return leftSize * rightSize;
    }
  }

  @Override
  protected void executeOperationStep() {
    startWorkTime();
    switch (joinType) {
      case JOIN:
      case CARTESIAN_PRODUCT:
        executeJoinStep();
        break;
      case LEFT_FORWARD:
        executeLeftForwardStep();
        break;
      case RIGHT_FORWARD:
        executeRightForwardStep();
        break;
    }
    startIdleTime();
  }

  private void executeJoinStep() {
    for (int i = 0; i < getEmittedMappingsPerRound(); i++) {
      if ((iterator == null) || !iterator.hasNext()) {
        if (iterator != null) {
          numberOfComparisons += iterator.getNumberOfComparisons();
          iterator = null;
        }
        if (shouldConsumefromLeftChild()) {
          if (isInputQueueEmpty(0)) {
            if (isInputQueueEmpty(1)) {
              // there are no mappings to consume
              break;
            }
          } else {
            Mapping mapping = consumeMapping(0);
            if (mapping == null) {
              continue;
            }
            long[] mappingVars = ((QueryOperatorBase) getChildTask(0)).getResultVariables();
            long[] rightVars = ((QueryOperatorBase) getChildTask(1)).getResultVariables();
            leftMappingCache.add(mapping);
            iterator = new JoinIterator(recycleCache, getResultVariables(), joinVars, mapping,
                    mappingVars,
                    joinType == JoinType.CARTESIAN_PRODUCT ? rightMappingCache.iterator()
                            : rightMappingCache.getMatchCandidates(mapping, mappingVars),
                    rightVars);
          }
        } else {
          if (isInputQueueEmpty(1)) {
            if (isInputQueueEmpty(0)) {
              // there are no mappings to consume
              break;
            }
          } else {
            Mapping mapping = consumeMapping(1);
            if (mapping == null) {
              continue;
            }
            long[] mappingVars = ((QueryOperatorBase) getChildTask(1)).getResultVariables();
            long[] leftVars = ((QueryOperatorBase) getChildTask(0)).getResultVariables();
            rightMappingCache.add(mapping);
            iterator = new JoinIterator(recycleCache, getResultVariables(), joinVars, mapping,
                    mappingVars,
                    joinType == JoinType.CARTESIAN_PRODUCT ? leftMappingCache.iterator()
                            : leftMappingCache.getMatchCandidates(mapping, mappingVars),
                    leftVars);
          }
        }
        i--;
      } else {
        Mapping resultMapping = iterator.next();
        emitMapping(resultMapping);
      }
    }
  }

  private boolean shouldConsumefromLeftChild() {
    if (isInputQueueEmpty(1)) {
      return true;
    } else if (isInputQueueEmpty(0)) {
      return false;
    } else {
      return leftMappingCache.size() < rightMappingCache.size();
    }
  }

  private void executeLeftForwardStep() {
    if (hasChildFinished(1)) {
      // the right child has finished
      if (isInputQueueEmpty(1)) {
        // no match for the right expression could be found
        // discard all mappings received from left child
        while (!isInputQueueEmpty(0)) {
          Mapping mapping = consumeMapping(0);
          if (mapping == null) {
            continue;
          }
          recycleCache.releaseMapping(mapping);
        }
      } else {
        // the right child has matched
        if (!isInputQueueEmpty(0)) {
          for (int i = 0; (i < getEmittedMappingsPerRound()) && !isInputQueueEmpty(0); i++) {
            Mapping mapping = consumeMapping(0);
            if (mapping == null) {
              break;
            }
            emitMapping(mapping);
          }
        }
        if (hasChildFinished(0) && isInputQueueEmpty(0)) {
          // as a final step, discard the empty mapping from the right
          // child
          Mapping mapping = consumeMapping(1);
          if (mapping != null) {
            recycleCache.releaseMapping(mapping);
          }
        }
      }
    }
  }

  private void executeRightForwardStep() {
    if (hasChildFinished(0)) {
      // the left child has finished
      if (isInputQueueEmpty(0)) {
        // no match for the left expression could be found
        // discard all mappings received from right child
        while (!isInputQueueEmpty(1)) {
          Mapping mapping = consumeMapping(1);
          if (mapping == null) {
            continue;
          }
          recycleCache.releaseMapping(mapping);
        }
      } else {
        // the left child has matched
        if (!isInputQueueEmpty(1)) {
          for (int i = 0; (i < getEmittedMappingsPerRound()) && !isInputQueueEmpty(1); i++) {
            Mapping mapping = consumeMapping(1);
            if (mapping == null) {
              break;
            }
            emitMapping(mapping);
          }
        }
        if (hasChildFinished(1) && isInputQueueEmpty(1)) {
          // as a final step, discard the empty mapping from the left
          // child
          Mapping mapping = consumeMapping(0);
          if (mapping != null) {
            recycleCache.releaseMapping(mapping);
          }
        }
      }
    }
  }

  @Override
  protected boolean isFinishedLocally() {
    return super.isFinishedLocally() && ((iterator == null) || !iterator.hasNext());
  }

  @Override
  protected void closeInternal() {
    leftMappingCache.close();
    rightMappingCache.close();
  }

  @Override
  public void serialize(DataOutputStream output, boolean useBaseImplementation, int slaveId)
          throws IOException {
    if (getParentTask() == null) {
      output.writeBoolean(useBaseImplementation);
      output.writeLong(getCoordinatorID());
    }
    output.writeInt(QueryOperatorType.TRIPLE_PATTERN_JOIN.ordinal());
    ((QueryOperatorTask) getChildTask(0)).serialize(output, useBaseImplementation, slaveId);
    ((QueryOperatorTask) getChildTask(1)).serialize(output, useBaseImplementation, slaveId);
    output.writeLong(getIdOnSlave(slaveId));
    output.writeInt(getEmittedMappingsPerRound());
    output.writeLong(getEstimatedTaskLoad());
  }

  @Override
  public void toString(StringBuilder sb, int indention) {
    indent(sb, indention);
    sb.append(getClass().getSimpleName());
    sb.append(" ").append(joinType.name());
    sb.append(" joinVars: [");
    String delim = "";
    for (long var : joinVars) {
      sb.append(delim).append(var);
      delim = ",";
    }
    sb.append("]");
    sb.append(" resultVars: [");
    delim = "";
    for (long var : resultVars) {
      sb.append(delim).append(var);
      delim = ",";
    }
    sb.append("]");
    sb.append(" estimatedWorkLoad: ").append(getEstimatedTaskLoad());
    sb.append("\n");
    ((QueryOperatorBase) getChildTask(0)).toString(sb, indention + 1);
    ((QueryOperatorBase) getChildTask(1)).toString(sb, indention + 1);
  }

  @Override
  public String toAlgebraicString() {
    StringBuilder sb = new StringBuilder();
    sb.append("join(");
    sb.append(getChildTask(0).getID() & 0xff_ffL);
    sb.append(",").append(getChildTask(1).getID() & 0xff_ffL);
    sb.append(",").append(joinType);
    sb.append(")");
    return sb.toString();
  }

  @Override
  public void close() {
    super.close();
    if (measurementCollector != null) {
      if (iterator != null) {
        numberOfComparisons += iterator.getNumberOfComparisons();
        iterator = null;
      }
      measurementCollector.measureValue(MeasurementType.QUERY_OPERATION_JOIN_NUMBER_OF_COMPARISONS,
              Integer.toString((int) (getID() >>> Short.SIZE)), Long.toString(getID() & 0xff_ffL),
              Long.toString(numberOfComparisons));
    }
  }

  private static enum JoinType {
    JOIN, CARTESIAN_PRODUCT, LEFT_FORWARD, RIGHT_FORWARD;
  }
}
