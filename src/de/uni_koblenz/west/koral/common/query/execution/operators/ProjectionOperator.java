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
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorBase;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorType;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

/**
 * Performs the projection operation.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class ProjectionOperator extends QueryOperatorBase {

  private final long[] resultVars;

  public ProjectionOperator(long id, long coordinatorId, int numberOfSlaves, int cacheSize,
          File cacheDirectory, int emittedMappingsPerRound, long[] resultVars,
          QueryOperatorTask subOperation) {
    super(id, coordinatorId, numberOfSlaves, cacheSize, cacheDirectory, emittedMappingsPerRound);
    this.resultVars = resultVars;
    addChildTask(subOperation);
  }

  public ProjectionOperator(short slaveId, int queryId, short taskId, long coordinatorId,
          int numberOfSlaves, int cacheSize, File cacheDirectory, int emittedMappingsPerRound,
          long[] resultVars, QueryOperatorTask subOperation) {
    super(slaveId, queryId, taskId, coordinatorId, numberOfSlaves, cacheSize, cacheDirectory,
            emittedMappingsPerRound);
    this.resultVars = resultVars;
    addChildTask(subOperation);
  }

  @Override
  public long computeEstimatedLoad(GraphStatistics statistics, int slave, boolean setLoads) {
    QueryOperatorBase subOp = (QueryOperatorBase) getChildTask(0);
    long load = subOp.computeEstimatedLoad(statistics, slave, setLoads);
    if (setLoads) {
      setEstimatedWorkLoad(load);
    }
    return load;
  }

  @Override
  public long computeTotalEstimatedLoad(GraphStatistics statistics) {
    QueryOperatorBase subOp = (QueryOperatorBase) getChildTask(0);
    return subOp.computeTotalEstimatedLoad(statistics);
  }

  @Override
  public long[] getResultVariables() {
    return resultVars;
  }

  @Override
  public long getFirstJoinVar() {
    long min = Long.MAX_VALUE;
    for (long var : resultVars) {
      if (var < min) {
        min = var;
      }
    }
    return min;
  }

  @Override
  public long getCurrentTaskLoad() {
    return getSizeOfInputQueue(0);
  }

  @Override
  protected void closeInternal() {
  }

  @Override
  protected void executeOperationStep() {
    startWorkTime();
    for (int i = 0; (i < getEmittedMappingsPerRound()) && !isInputQueueEmpty(0); i++) {
      Mapping mapping = consumeMapping(0);
      if (mapping != null) {
        Mapping result = recycleCache.getMappingWithRestrictedVariables(mapping,
                ((QueryOperatorBase) getChildTask(0)).getResultVariables(), resultVars);
        emitMapping(result);
        recycleCache.releaseMapping(mapping);
      }
    }
    startIdleTime();
  }

  @Override
  public void serialize(DataOutputStream output, boolean useBaseImplementation, int slaveId)
          throws IOException {
    if (getParentTask() == null) {
      output.writeBoolean(useBaseImplementation);
      output.writeLong(getCoordinatorID());
    }
    output.writeInt(QueryOperatorType.PROJECTION.ordinal());
    ((QueryOperatorTask) getChildTask(0)).serialize(output, useBaseImplementation, slaveId);
    output.writeLong(getIdOnSlave(slaveId));
    output.writeInt(getEmittedMappingsPerRound());
    output.writeLong(getEstimatedTaskLoad());
    output.writeInt(resultVars.length);
    for (int i = 0; i < resultVars.length; i++) {
      output.writeLong(resultVars[i]);
    }
  }

  @Override
  public void toString(StringBuilder sb, int indention) {
    indent(sb, indention);
    sb.append(getClass().getSimpleName());
    sb.append(" resultVars: [");
    String delim = "";
    for (long var : getResultVariables()) {
      sb.append(delim).append("?").append(var);
      delim = ",";
    }
    sb.append("]");
    sb.append(" estimatedWorkLoad: ").append(getEstimatedTaskLoad());
    sb.append("\n");
    ((QueryOperatorBase) getChildTask(0)).toString(sb, indention + 1);
  }

  @Override
  public String toAlgebraicString() {
    StringBuilder sb = new StringBuilder();
    sb.append("project(");
    sb.append(getChildTask(0).getID() & 0xff_ffL);
    for (long resultVar : getResultVariables()) {
      sb.append(",?").append(resultVar);
    }
    sb.append(")");
    return sb.toString();
  }

}
