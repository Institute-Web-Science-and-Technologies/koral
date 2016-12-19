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
import de.uni_koblenz.west.koral.common.query.TriplePattern;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorBase;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorType;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.koral.slave.triple_store.TripleStoreAccessor;
import de.uni_koblenz.west.koral.slave.triple_store.impl.MappingIteratorWrapper;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * Performs the match of a triple pattern.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class TriplePatternMatchOperator extends QueryOperatorBase {

  private final TriplePattern pattern;

  private final TripleStoreAccessor tripleStore;

  private Iterator<Mapping> iterator;

  public TriplePatternMatchOperator(long id, long coordinatorId, int numberOfSlaves, int cacheSize,
          File cacheDirectory, TriplePattern pattern, int emittedMappingsPerRound,
          TripleStoreAccessor tripleStore) {
    super(id, coordinatorId, numberOfSlaves, cacheSize, cacheDirectory, emittedMappingsPerRound);
    this.pattern = pattern;
    this.tripleStore = tripleStore;
  }

  public TriplePatternMatchOperator(short slaveId, int queryId, short taskId, long coordinatorId,
          int numberOfSlaves, int cacheSize, File cacheDirectory, TriplePattern pattern,
          int emittedMappingsPerRound, TripleStoreAccessor tripleStore) {
    super(slaveId, queryId, taskId, coordinatorId, numberOfSlaves, cacheSize, cacheDirectory,
            emittedMappingsPerRound);
    this.pattern = pattern;
    this.tripleStore = tripleStore;
  }

  @Override
  public long computeEstimatedLoad(GraphStatistics statistics, int slave, boolean setLoads) {
    long load = 0;
    switch (pattern.getType()) {
      case ___:
        long[] loads = statistics.getChunkSizes();
        if (slave < 0) {
          for (long load_i : loads) {
            load += load_i;
          }
        } else {
          load = loads[slave];
        }
        break;
      case S__:
        load = slave < 0 ? statistics.getTotalSubjectFrequency(pattern.getSubject())
                : statistics.getSubjectFrequency(pattern.getSubject(), slave);
        break;
      case _P_:
        load = slave < 0 ? statistics.getTotalPropertyFrequency(pattern.getProperty())
                : statistics.getPropertyFrequency(pattern.getProperty(), slave);
        break;
      case __O:
        load = slave < 0 ? statistics.getTotalObjectFrequency(pattern.getObject())
                : statistics.getObjectFrequency(pattern.getObject(), slave);
        break;
      case SP_:
        long subjectFrequency = slave < 0
                ? statistics.getTotalSubjectFrequency(pattern.getSubject())
                : statistics.getSubjectFrequency(pattern.getSubject(), slave);
        if (subjectFrequency != 0) {
          long propertyFrequency = slave < 0
                  ? statistics.getTotalPropertyFrequency(pattern.getProperty())
                  : statistics.getPropertyFrequency(pattern.getProperty(), slave);
          if (subjectFrequency < propertyFrequency) {
            load = subjectFrequency;
          } else {
            load = propertyFrequency;
          }
        }
        break;
      case S_O:
        subjectFrequency = slave < 0 ? statistics.getTotalSubjectFrequency(pattern.getSubject())
                : statistics.getSubjectFrequency(pattern.getSubject(), slave);
        if (subjectFrequency != 0) {
          long objectFrequency = slave < 0 ? statistics.getTotalObjectFrequency(pattern.getObject())
                  : statistics.getObjectFrequency(pattern.getObject(), slave);
          if (subjectFrequency < objectFrequency) {
            load = subjectFrequency;
          } else {
            load = objectFrequency;
          }
        }
        break;
      case _PO:
        long propertyFrequency = slave < 0
                ? statistics.getTotalPropertyFrequency(pattern.getProperty())
                : statistics.getPropertyFrequency(pattern.getProperty(), slave);
        if (propertyFrequency != 0) {
          long objectFrequency = slave < 0 ? statistics.getTotalObjectFrequency(pattern.getObject())
                  : statistics.getObjectFrequency(pattern.getObject(), slave);
          if (propertyFrequency < objectFrequency) {
            load = propertyFrequency;
          } else {
            load = objectFrequency;
          }
        }
        break;
      case SPO:
        subjectFrequency = slave < 0 ? statistics.getTotalSubjectFrequency(pattern.getSubject())
                : statistics.getSubjectFrequency(pattern.getSubject(), slave);
        if (subjectFrequency != 0) {
          propertyFrequency = slave < 0
                  ? statistics.getTotalPropertyFrequency(pattern.getProperty())
                  : statistics.getPropertyFrequency(pattern.getProperty(), slave);
          if (propertyFrequency != 0) {
            long objectFrequency = slave < 0
                    ? statistics.getTotalObjectFrequency(pattern.getObject())
                    : statistics.getObjectFrequency(pattern.getObject(), slave);
            if (objectFrequency == 0) {
              load = 0;
            } else {
              load = 1;
            }
          }
        }
        break;
    }
    if (setLoads) {
      setEstimatedWorkLoad(load);
    }
    return load;
  }

  @Override
  public long computeTotalEstimatedLoad(GraphStatistics statistics) {
    return computeEstimatedLoad(statistics, -1);
  }

  @Override
  public long[] getResultVariables() {
    return pattern.getVariables();
  }

  @Override
  public long getFirstJoinVar() {
    long[] vars = pattern.getVariables();
    long min = Long.MAX_VALUE;
    for (long var : vars) {
      if (var < min) {
        min = var;
      }
    }
    return min;
  }

  @Override
  public long getCurrentTaskLoad() {
    if ((iterator == null) || (tripleStore == null) || (getEstimatedTaskLoad() == 0)
            || !iterator.hasNext()) {
      return 0;
    } else {
      return getEmittedMappingsPerRound();
    }
  }

  @Override
  protected void closeInternal() {
    if (iterator instanceof MappingIteratorWrapper) {
      ((MappingIteratorWrapper) iterator).close();
    }
  }

  @Override
  protected void executeOperationStep() {
    startWorkTime();
    if ((getEstimatedTaskLoad() == 0) || (tripleStore == null)) {
      return;
    }
    if (iterator == null) {
      iterator = tripleStore.lookup(recycleCache, pattern).iterator();
    }
    for (int i = 0; (i < getEmittedMappingsPerRound()) && iterator.hasNext(); i++) {
      Mapping mapping = iterator.next();
      emitMapping(mapping);
    }
    startIdleTime();
  }

  @Override
  protected boolean isFinishedLocally() {
    return (getEstimatedTaskLoad() == 0) || (tripleStore == null)
            || ((iterator != null) && !iterator.hasNext());
  }

  @Override
  public void serialize(DataOutputStream output, boolean useBaseImplementation, int slaveId)
          throws IOException {
    if (getParentTask() == null) {
      output.writeBoolean(useBaseImplementation);
      output.writeLong(getCoordinatorID());
    }
    output.writeInt(QueryOperatorType.TRIPLE_PATTERN_MATCH.ordinal());
    output.writeLong(getIdOnSlave(slaveId));
    output.writeInt(getEmittedMappingsPerRound());
    output.writeLong(getEstimatedTaskLoad());
    output.writeInt(pattern.getType().ordinal());
    output.writeLong(pattern.getSubject());
    output.writeLong(pattern.getProperty());
    output.writeLong(pattern.getObject());
  }

  @Override
  public void toString(StringBuilder sb, int indention) {
    indent(sb, indention);
    sb.append(getClass().getSimpleName());
    sb.append(" pattern: <");
    sb.append(pattern.isSubjectVariable() ? "?" : "").append(pattern.getSubject());
    sb.append(" ").append(pattern.isPropertyVariable() ? "?" : "").append(pattern.getProperty());
    sb.append(" ").append(pattern.isObjectVariable() ? "?" : "").append(pattern.getObject());
    sb.append(">");
    sb.append(" estimatedWorkLoad: ").append(getEstimatedTaskLoad());
    sb.append("\n");
  }

  @Override
  public String toAlgebraicString() {
    StringBuilder sb = new StringBuilder();
    sb.append("match(");
    if (pattern.isSubjectVariable()) {
      sb.append("?");
    }
    sb.append(pattern.getSubject());
    sb.append(",");
    if (pattern.isPropertyVariable()) {
      sb.append("?");
    }
    sb.append(pattern.getProperty());
    sb.append(",");
    if (pattern.isObjectVariable()) {
      sb.append("?");
    }
    sb.append(pattern.getObject());
    sb.append(")");
    return sb.toString();
  }

}
