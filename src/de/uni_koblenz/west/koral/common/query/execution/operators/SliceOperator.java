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

import de.uni_koblenz.west.koral.common.query.execution.QueryExecutionCoordinator;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorBase;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

/**
 * This class represents the result modifier offset and limit. This class is
 * only used for parsing purposes. During the execution this is done by
 * {@link QueryExecutionCoordinator}. Offset is quite useless, since the
 * ordering of results cannot be guaranteed to be the same for different
 * executions.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class SliceOperator extends QueryOperatorBase {

  private final long offset;

  private final long length;

  public SliceOperator(short slaveId, int queryId, short taskId, long coordinatorId,
          int numberOfSlaves, int cacheSize, File cacheDirectory, int emittedMappingsPerRound,
          QueryOperatorTask subOperation, long offset, long length) {
    super(slaveId, queryId, taskId, coordinatorId, numberOfSlaves, cacheSize, cacheDirectory,
            emittedMappingsPerRound);
    addChildTask(subOperation);
    this.offset = offset;
    this.length = length;
  }

  public SliceOperator(long id, long coordinatorId, int numberOfSlaves, int cacheSize,
          File cacheDirectory, int emittedMappingsPerRound, QueryOperatorTask subOperation,
          long offset, long length) {
    super(id, coordinatorId, numberOfSlaves, cacheSize, cacheDirectory, emittedMappingsPerRound);
    addChildTask(subOperation);
    this.offset = offset;
    this.length = length;
  }

  public long getOffset() {
    return offset;
  }

  public long getLength() {
    return length;
  }

  @Override
  public long[] getResultVariables() {
    return ((QueryOperatorTask) getChildTask(0)).getResultVariables();
  }

  @Override
  public long getFirstJoinVar() {
    long min = Long.MAX_VALUE;
    for (long var : getResultVariables()) {
      if (var < min) {
        min = var;
      }
    }
    return min;
  }

  @Override
  public void serialize(DataOutputStream output, boolean useBaseImplementation, int slaveId)
          throws IOException {
    if (getParentTask() == null) {
      output.writeBoolean(useBaseImplementation);
      output.writeLong(getCoordinatorID());
    }
    // this class is only used during the parsing process
    QueryOperatorBase subOp = (QueryOperatorBase) getChildTask(0);
    subOp.serialize(output, useBaseImplementation, slaveId);
  }

  @Override
  public long getCurrentTaskLoad() {
    return getSizeOfInputQueue(0);
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
  public void toString(StringBuilder sb, int indention) {
    indent(sb, indention);
    sb.append(getClass().getSimpleName());
    sb.append(" offset: ").append(getOffset());
    sb.append(" limit: ").append(getLength());
    sb.append(" estimatedWorkLoad: ").append(getEstimatedTaskLoad());
    sb.append("\n");
    ((QueryOperatorBase) getChildTask(0)).toString(sb, indention + 1);
  }

  @Override
  public String toAlgebraicString() {
    StringBuilder sb = new StringBuilder();
    sb.append("slice(");
    sb.append(getChildTask(0).getID() & 0xff_ffL);
    sb.append(",").append(getOffset());
    sb.append(",").append(getLength());
    sb.append(")");
    return sb.toString();
  }

  @Override
  protected void executeOperationStep() {
    throw new UnsupportedOperationException(
            "The slice operation is performed by the query execution coordinator.");
  }

  @Override
  protected void closeInternal() {
  }

}
