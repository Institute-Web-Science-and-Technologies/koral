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
import de.uni_koblenz.west.koral.common.query.Mapping;
import de.uni_koblenz.west.koral.common.query.MappingRecycleCache;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorBase;
import de.uni_koblenz.west.koral.common.utils.CachedFileReceiverQueue;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * A base implementation of the {@link WorkerTask}. It implements the following
 * features:
 * <ul>
 * <li>a unique id</li>
 * <li>handling of child tasks</li>
 * <li>providing an arbitrary number of input queues for incoming messages</li>
 * <ul>
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public abstract class WorkerTaskBase implements WorkerTask {

  protected Logger logger;

  protected MeasurementCollector measurementCollector;

  private final long id;

  private CachedFileReceiverQueue[] inputQueues;

  private final int cacheSize;

  private final File cacheDirectory;

  private WorkerTask[] children;

  private final AtomicBoolean areChildrenFinished;

  public WorkerTaskBase(long id, int cacheSize, File cacheDirectory) {
    this.id = id;
    this.cacheSize = cacheSize;
    this.cacheDirectory = new File((cacheDirectory != null ? cacheDirectory
            : new File(System.getProperty("java.io.tmpdir"))).getAbsolutePath() + File.separatorChar
            + "workerTask_" + this.id);
    areChildrenFinished = new AtomicBoolean(false);
  }

  @Override
  public void setUp(MessageSenderBuffer messageSender, MappingRecycleCache recycleCache,
          Logger logger, MeasurementCollector measurementCollector) {
    this.logger = logger;
    this.measurementCollector = measurementCollector;
  }

  @Override
  public long getID() {
    return id;
  }

  protected File getCacheDirectory() {
    return cacheDirectory;
  }

  protected void addInputQueue() {
    if ((inputQueues == null) || (inputQueues.length == 0)) {
      inputQueues = new CachedFileReceiverQueue[1];
    } else {
      CachedFileReceiverQueue[] newInputQueues = new CachedFileReceiverQueue[inputQueues.length
              + 1];
      for (int i = 0; i < inputQueues.length; i++) {
        newInputQueues[i] = inputQueues[i];
      }
      inputQueues = newInputQueues;
    }
    inputQueues[inputQueues.length - 1] = new CachedFileReceiverQueue(cacheSize, cacheDirectory,
            inputQueues.length - 1);
  }

  @Override
  public boolean hasInput() {
    if (inputQueues != null) {
      for (CachedFileReceiverQueue queue : inputQueues) {
        if (!queue.isEmpty()) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean hasToPerformFinalSteps() {
    return false;
  }

  protected long getSizeOfInputQueue(int inputQueueIndex) {
    return inputQueues[inputQueueIndex].size();
  }

  protected void enqueuMessage(int inputQueueIndex, byte[] message, int firstIndex, int length) {
    if (inputQueues[inputQueueIndex].isClosed()) {
      if (logger != null) {
        logger.finer("Discarding a message because the queue of task " + getID()
                + " was already closed.");
      }
    } else {
      inputQueues[inputQueueIndex].enqueue(message, firstIndex, length);
    }
  }

  protected Mapping consumeMapping(int inputQueueIndex, MappingRecycleCache recycleCache) {
    if (!inputQueues[inputQueueIndex].isClosed()) {
      return inputQueues[inputQueueIndex].dequeue(recycleCache);
    } else {
      return null;
    }
  }

  protected boolean isInputQueueEmpty(int inputQueueIndex) {
    return inputQueues[inputQueueIndex].isEmpty();
  }

  public int addChildTask(WorkerTask child) {
    int id = 0;
    if ((children == null) || (children.length == 0)) {
      children = new WorkerTask[1];
    } else {
      WorkerTask[] newChildren = new WorkerTask[children.length + 1];
      for (int i = 0; i < children.length; i++) {
        newChildren[i] = children[i];
      }
      children = newChildren;
      id = children.length - 1;
    }
    children[id] = child;
    addInputQueue();
    return id;
  }

  @Override
  public Set<WorkerTask> getPrecedingTasks() {
    Set<WorkerTask> precedingTasks = new HashSet<>();
    if (children != null) {
      for (WorkerTask child : children) {
        precedingTasks.add(child);
      }
    }
    return precedingTasks;
  }

  protected int getIndexOfChild(long childId) {
    for (int childIndex = 0; childIndex < children.length; childIndex++) {
      if (children[childIndex].getID() == childId) {
        return childIndex;
      }
    }
    return -1;
  }

  protected WorkerTask getChildTask(int i) {
    if ((children == null) || (children.length == 0)) {
      return null;
    } else {
      return children[i];
    }
  }

  public WorkerTask[] getChildren() {
    if (children != null) {
      return Arrays.copyOf(children, children.length);
    } else {
      return new WorkerTask[0];
    }
  }

  /**
   * Called by subclasses of {@link QueryOperatorBase}.
   * 
   * @param child
   * @return <code>true</code> if the child operation is finished.
   */
  protected boolean hasChildFinished(int child) {
    return children[child].isInFinalState();
  }

  protected boolean areAllChildrenFinished() {
    return areChildrenFinished.get();
  }

  protected void updateChildrenFinished() {
    if (children != null) {
      for (WorkerTask child : children) {
        if (!child.isInFinalState()) {
          return;
        }
      }
    }
    areChildrenFinished.set(true);
  }

  @Override
  public void close() {
    if (inputQueues != null) {
      for (CachedFileReceiverQueue queue : inputQueues) {
        queue.close();
      }
    }
  }

  @Override
  public String toString() {
    return getClass().getName() + "[id=" + id + "(slave=" + (id >>> (Integer.SIZE + Short.SIZE))
            + " query=" + ((id << Short.SIZE) >>> (Short.SIZE + Short.SIZE)) + " task="
            + ((id << (Short.SIZE + Integer.SIZE)) >>> (Short.SIZE + Integer.SIZE)) + ")]";
  }

}
