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

import de.uni_koblenz.west.koral.common.executor.WorkerTask;
import de.uni_koblenz.west.koral.common.executor.messagePassing.MessageSenderBuffer;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.measurement.MeasurementType;
import de.uni_koblenz.west.koral.common.query.Mapping;
import de.uni_koblenz.west.koral.common.query.MappingRecycleCache;
import de.uni_koblenz.west.koral.common.query.execution.operators.ProjectionOperator;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * This is the base implementation of {@link WorkerTask} that is common for all
 * query operations.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public abstract class QueryOperatorBase extends QueryTaskBase implements QueryOperatorTask {

  private final long coordinatorId;

  private QueryOperatorBase parent;

  private final int emittedMappingsPerRound;

  /*
   * Performance measurements
   */

  private long startIdleTime;

  private long totalIdleTime;

  private long startWorkTime;

  private long totalWorkTime;

  /**
   * master,slave0,slave1,...
   */
  protected long[] numberOfEmittedMappings;

  public QueryOperatorBase(short slaveId, int queryId, short taskId, long coordinatorId,
          int numberOfSlaves, int cacheSize, File cacheDirectory, int emittedMappingsPerRound) {
    super((((((long) slaveId) << Integer.SIZE)
            | (queryId & 0x00_00_00_00_ff_ff_ff_ffl)) << Short.SIZE)
            | (taskId & 0x00_00_00_00_00_00_ff_ffl), numberOfSlaves, cacheSize, cacheDirectory);
    this.coordinatorId = coordinatorId;
    this.emittedMappingsPerRound = emittedMappingsPerRound;
  }

  public QueryOperatorBase(long id, long coordinatorId, int numberOfSlaves, int cacheSize,
          File cacheDirectory, int emittedMappingsPerRound) {
    super(id, numberOfSlaves, cacheSize, cacheDirectory);
    this.coordinatorId = coordinatorId;
    this.emittedMappingsPerRound = emittedMappingsPerRound;
  }

  @Override
  public void setUp(MessageSenderBuffer messageSender, MappingRecycleCache recycleCache,
          Logger logger, MeasurementCollector measurementCollector) {
    numberOfEmittedMappings = new long[messageSender.getNumberOfSlaves() + 1];
    super.setUp(messageSender, recycleCache, logger, measurementCollector);
  }

  /**
   * @param statistics
   * @param slave
   *          the first slave has id 0
   * @return
   */
  public long computeEstimatedLoad(GraphStatistics statistics, int slave) {
    return computeEstimatedLoad(statistics, slave, false);
  }

  public abstract long computeEstimatedLoad(GraphStatistics statistics, int slave,
          boolean setLoads);

  public abstract long computeTotalEstimatedLoad(GraphStatistics statistics);

  public void adjustEstimatedLoad(GraphStatistics statistics, int slave) {
    computeEstimatedLoad(statistics, slave, true);
  }

  @Override
  public long getCoordinatorID() {
    return coordinatorId;
  }

  protected int getEmittedMappingsPerRound() {
    return emittedMappingsPerRound;
  }

  public void setParentTask(WorkerTask parent) {
    if ((parent == null) || !(parent instanceof QueryOperatorBase)) {
      throw new IllegalArgumentException(
              "The parent worker task must be of type " + getClass().getName());
    }
    this.parent = (QueryOperatorBase) parent;
  }

  @Override
  public WorkerTask getParentTask() {
    return parent;
  }

  @Override
  public void start() {
    startTimeMeasurement();
    startIdleTime();
    super.start();
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.QUERY_OPERATION_START,
              System.currentTimeMillis(), Integer.toString((int) (getID() >>> Short.SIZE)),
              Long.toString(getID() & 0xff_ffL));
    }
  }

  @Override
  protected void handleFinishNotification(long sender, Object object, int firstIndex,
          int messageLength) {
  }

  @Override
  protected void handleMappingReception(long sender, byte[] message, int firstIndex, int length) {
    long taskId = (sender & 0x00_00_ff_ff_ff_ff_ff_ffl) | (getID() & 0xff_ff_00_00_00_00_00_00l);
    int childIndex = getIndexOfChild(taskId);
    enqueuMessage(childIndex, message, firstIndex, length);
  }

  @Override
  public boolean hasInput() {
    if (getChildTask(0) == null) {
      return !isFinishedLocally();
    } else {
      return super.hasInput();
    }
  }

  @Override
  protected boolean isFinishedLocally() {
    // prevent infinite loop if leefs in query execution tree do not change
    // implementation of this method
    return !super.hasInput();
  }

  @Override
  public boolean hasToPerformFinalSteps() {
    if (getChildTask(0) == null) {
      return isFinishedLocally() || (getEstimatedTaskLoad() == 0);
    } else {
      return super.hasToPerformFinalSteps();
    }
  }

  @Override
  protected void executePreStartStep() {
  }

  @Override
  protected void executeFinalStep() {
    messageSender.sendQueryTaskFinished(getID(), getParentTask() == null, getCoordinatorID(),
            recycleCache);
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.QUERY_OPERATION_LOCAL_FINISH,
              System.currentTimeMillis(), Integer.toString((int) (getID() >>> Short.SIZE)),
              Long.toString(getID() & 0xff_ffL));
    }
  }

  @Override
  protected void tidyUp() {
    stopTimeMeasurement();
    if (measurementCollector != null) {
      long taskId = getID() & 0xff_ffL;
      String[] values = new String[4 + numberOfEmittedMappings.length];
      values[0] = Integer.toString((int) (getID() >>> Short.SIZE));
      values[1] = Long.toString(taskId);
      values[2] = Long.toString(totalIdleTime);
      values[3] = Long.toString(totalWorkTime);
      for (int i = 0; i < numberOfEmittedMappings.length; i++) {
        values[4 + i] = Long.toString(numberOfEmittedMappings[i]);
      }
      measurementCollector.measureValue(MeasurementType.QUERY_OPERATION_FINISH,
              System.currentTimeMillis(), values);
    }
  }

  /**
   * Called by subclasses of {@link QueryOperatorBase}.<br>
   * Sends <code>mapping</code> to the {@link #parent} operator. If this is the
   * root operator, it sends the mapping to the query coordinator. See
   * definition of target' function (definition 31).
   * 
   * @param mapping
   */
  protected void emitMapping(Mapping mapping) {
    if (getParentTask() == null) {
      messageSender.sendQueryMapping(mapping, getID(), getCoordinatorID(), recycleCache);
      numberOfEmittedMappings[0]++;
    } else if (getParentTask() instanceof ProjectionOperator) {
      // projection operator filters all mappings on the same computer
      messageSender.sendQueryMapping(mapping, getID(), getParentTask().getID(), recycleCache);
      numberOfEmittedMappings[(int) (getParentTask().getID() >>> (Integer.SIZE + Short.SIZE))]++;
    } else {
      short thisComputerID = (short) (getID() >>> (Short.SIZE + Integer.SIZE));
      long parentBaseID = getParentTask().getID() & 0x00_00_FF_FF_FF_FF_FF_FFl;
      if (mapping.isEmptyMapping()) {
        if (mapping.getIdOfFirstComputerKnowingThisMapping() == thisComputerID) {
          // the first computer who knows this empty mapping, forwards
          // it to all parent tasks
          mapping.setContainmentToAll();
          messageSender.sendQueryMappingToAll(mapping, getID(), parentBaseID, recycleCache);
          for (int i = 1; i < numberOfEmittedMappings.length; i++) {
            numberOfEmittedMappings[i]++;
          }
        }
      } else {
        long firstJoinVar = ((QueryOperatorTask) getParentTask()).getFirstJoinVar();
        if (firstJoinVar == -1) {
          // parent task has no join variables
          // send to computer with smallest id
          mapping.updateContainment((int) (getID() >>> (Short.SIZE + Integer.SIZE)), 1);
          messageSender.sendQueryMapping(mapping, getID(),
                  parentBaseID | 0x00_01_00_00_00_00_00_00l, recycleCache);
          numberOfEmittedMappings[1]++;
        } else {
          long ownerLong = mapping.getValue(firstJoinVar, getResultVariables())
                  & 0xFF_FF_00_00_00_00_00_00l;
          int owner = ((int) (ownerLong >>> (Short.SIZE + Integer.SIZE))) + 1;
          ownerLong = ((long) owner) << (Integer.SIZE + Short.SIZE);
          if (mapping.isKnownByComputer(owner)) {
            if (mapping.isKnownByComputer((int) (getID() >>> (Short.SIZE + Integer.SIZE)))) {
              // the owner also knows a replicate of this mapping,
              // forward it to parent task on this computer
              messageSender.sendQueryMapping(mapping, getID(), getParentTask().getID(),
                      recycleCache);
              numberOfEmittedMappings[(int) (getParentTask()
                      .getID() >>> (Integer.SIZE + Short.SIZE))]++;
            }
          } else {
            if (mapping.getIdOfFirstComputerKnowingThisMapping() == thisComputerID) {
              // first knowing computer sends mapping to owner
              // which is a remote computer
              mapping.updateContainment((int) (getID() >>> (Short.SIZE + Integer.SIZE)), owner);
              messageSender.sendQueryMapping(mapping, getID(), parentBaseID | ownerLong,
                      recycleCache);
              numberOfEmittedMappings[owner]++;
            }
          }
        }
      }
    }
  }

  protected void startTimeMeasurement() {
    startWorkTime = 0;
    totalWorkTime = 0;
    totalIdleTime = 0;
    startIdleTime = System.currentTimeMillis();
  }

  protected void startIdleTime() {
    if (startIdleTime < startWorkTime) {
      totalWorkTime += System.currentTimeMillis() - startWorkTime;
      startIdleTime = System.currentTimeMillis();
      if (startIdleTime == startWorkTime) {
        startWorkTime--;
      }
    }
  }

  protected void startWorkTime() {
    if (startWorkTime < startIdleTime) {
      totalIdleTime += System.currentTimeMillis() - startIdleTime;
      startWorkTime = System.currentTimeMillis();
      if (startIdleTime == startWorkTime) {
        startIdleTime--;
      }
    }
  }

  protected void stopTimeMeasurement() {
    if ((startIdleTime == 0) && (startWorkTime == 0)) {
      return;
    } else if (startIdleTime < startWorkTime) {
      startIdleTime();
      startWorkTime = 0;
      startIdleTime = 0;
    } else if (startWorkTime < startIdleTime) {
      startWorkTime();
      startWorkTime = 0;
      startIdleTime = 0;
    }
  }

  @Override
  public byte[] serialize(boolean useBaseImplementation, int slaveId) {
    ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
    try (DataOutputStream output = new DataOutputStream(byteOutput);) {
      serialize(output, useBaseImplementation, slaveId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return byteOutput.toByteArray();
  }

  protected long getIdOnSlave(int slaveId) {
    return ((getID() << Short.SIZE) >>> Short.SIZE)
            | (((long) slaveId) << (Integer.SIZE + Short.SIZE));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    toString(sb, 0);
    return sb.toString();
  }

  public abstract void toString(StringBuilder sb, int indention);

  public abstract String toAlgebraicString();

  protected void indent(StringBuilder sb, int indention) {
    for (int i = 0; i < indention; i++) {
      sb.append("    ");
    }
  }

  @Override
  public void close() {
    super.close();
    long taskId = getID() & 0xff_ffL;
    closeInternal();
    stopTimeMeasurement();
    if (measurementCollector != null) {
      String[] values = new String[4 + numberOfEmittedMappings.length];
      values[0] = Integer.toString((int) (getID() >>> Short.SIZE));
      values[1] = Long.toString(taskId);
      values[2] = Long.toString(totalIdleTime);
      values[3] = Long.toString(totalWorkTime);
      for (int i = 0; i < numberOfEmittedMappings.length; i++) {
        values[4 + i] = Long.toString(numberOfEmittedMappings[i]);
      }
      measurementCollector.measureValue(MeasurementType.QUERY_OPERATION_CLOSED,
              System.currentTimeMillis(), values);
      measurementCollector.measureValue(
              MeasurementType.QUERY_OPERATION_SENT_FINISH_NOTIFICATIONS_TO_OTHER_SLAVES,
              Integer.toString((int) (getID() >>> Short.SIZE)), Long.toString(taskId),
              Integer.toString(numberOfEmittedMappings.length - 2));
      values = new String[((2 + ((numberOfEmittedMappings.length - 2) * 2))) + 1];
      values[0] = Integer.toString((int) (getID() >>> Short.SIZE));
      values[1] = Long.toString(taskId);
      int nextIndex = 2;
      for (int i = 1; i < numberOfEmittedMappings.length; i++) {
        if (i != (getID() >>> (Short.SIZE + Integer.SIZE))) {
          values[nextIndex++] = Integer.toString(i);
          long emitted = numberOfEmittedMappings[i];
          values[nextIndex++] = Long.toString(emitted);
        }
      }
      values[values.length - 1] = Integer.toString(getResultVariables().length);
      measurementCollector.measureValue(MeasurementType.QUERY_OPERATION_SENT_MAPPINGS_TO_SLAVE,
              values);
      if (parent == null) {
        messageSender.measureSentMessages((int) (getID() >>> Short.SIZE));
      }
    }
  }

  protected abstract void closeInternal();

}
