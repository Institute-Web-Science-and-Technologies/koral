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
package de.uni_koblenz.west.koral.common.executor;

import de.uni_koblenz.west.koral.common.executor.messagePassing.MessageSenderBuffer;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.query.MappingRecycleCache;

import java.io.Closeable;
import java.util.Set;
import java.util.logging.Logger;

/**
 * This interface declares all methods required to execute a task in the
 * execution framework of Koral.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface WorkerTask extends Closeable {

  public void setUp(MessageSenderBuffer messageSender, MappingRecycleCache recycleCache,
          Logger logger, MeasurementCollector measurementCollector);

  /**
   * <p>
   * The id consists of:
   * <ol>
   * <li>2 bytes computer id</li>
   * <li>4 bytes query id</li>
   * <li>2 bytes query node id</li>
   * </ol>
   * </p>
   * 
   * <p>
   * The ids of two {@link WorkerTask}s should only be equal, if
   * task1.equals(task2)==true
   * </p>
   * 
   * @return
   */
  public long getID();

  /**
   * @return id of the query coordinator node of the query execution tree
   */
  public long getCoordinatorID();

  public long getEstimatedTaskLoad();

  public long getCurrentTaskLoad();

  public WorkerTask getParentTask();

  public Set<WorkerTask> getPrecedingTasks();

  /**
   * Starts to emit results on the next {@link #execute()} call.
   */
  public void start();

  public boolean hasInput();

  public boolean hasToPerformFinalSteps();

  public void enqueueMessage(long sender, byte[] message, int firstIndex, int lengthOfMessage);

  /**
   * Results may only be emitted, it {@link #start()} was called.
   */
  public void execute();

  /**
   * @return true, if task has successfully finished or if it was aborted
   */
  public boolean isInFinalState();

  @Override
  public void close();

  @Override
  public String toString();

}
