package de.uni_koblenz.west.koral.common.executor;

import de.uni_koblenz.west.koral.common.executor.messagePassing.MessageReceiverListener;
import de.uni_koblenz.west.koral.common.executor.messagePassing.MessageSenderBuffer;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.query.MappingRecycleCache;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

/**
 * Executes all registered {@link WorkerTask}s in iterations. During one
 * iteration {@link WorkerTask#execute()} is called once for each
 * {@link WorkerTask}. At the end of each iteration it forces to send all
 * buffered mappings and tries to reschedule tasks with its neighbors in order
 * to achieve a balanced workload during runtime.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class WorkerThread extends Thread implements Closeable, AutoCloseable {

  private final Logger logger;

  private final MeasurementCollector measurementCollector;

  private final int id;

  private final MappingRecycleCache mappingCache;

  private final MessageSenderBuffer messageSender;

  private final MessageReceiverListener receiver;

  private WorkerThread previous;

  private WorkerThread next;

  private final double unbalanceThreshold;

  private final ConcurrentLinkedQueue<WorkerTask> tasks;

  private long currentLoad;

  public WorkerThread(int id, int sizeOfMappingRecycleCache, double unbalanceThreshold,
          MessageReceiverListener receiver, MessageSenderBuffer messageSender, int numberOfSlaves,
          Logger logger, MeasurementCollector measurementCollector) {
    setDaemon(true);
    this.logger = logger;
    this.measurementCollector = measurementCollector;
    this.id = id;
    setName("WorkerThread " + id);
    tasks = new ConcurrentLinkedQueue<>();
    currentLoad = 0;
    mappingCache = new MappingRecycleCache(sizeOfMappingRecycleCache, numberOfSlaves);
    this.unbalanceThreshold = unbalanceThreshold;
    this.receiver = receiver;
    this.messageSender = messageSender;
  }

  private WorkerThread getPrevious() {
    return previous;
  }

  void setPrevious(WorkerThread previous) {
    this.previous = previous;
  }

  private WorkerThread getNext() {
    return next;
  }

  void setNext(WorkerThread next) {
    this.next = next;
  }

  public long getCurrentLoad() {
    return currentLoad;
  }

  MessageReceiverListener getReceiver() {
    return receiver;
  }

  public void addWorkerTask(WorkerTask task) {
    task.setUp(messageSender, mappingCache, logger, measurementCollector);
    receiver.register(task);
    receiveTask(task);
  }

  private void receiveTask(WorkerTask task) {
    tasks.offer(task);
    if (!isAlive()) {
      if (logger != null) {
        // TODO remove
        logger.info(getState().toString());
      }
      start();
    }
  }

  /**
   * Starts all query tasks with this query ID, independent of the
   * {@link WorkerThread} of the {@link WorkerManager} which owns this
   * {@link WorkerThread}.
   * 
   * @param receivedMessage
   */
  public void startQuery(byte[] receivedMessage) {
    for (WorkerTask task : receiver.getAllTasksOfQuery(receivedMessage, 1)) {
      task.start();
    }
  }

  public void abortQuery(byte[] receivedMessage) {
    messageSender.sendAllBufferedMessages(mappingCache);
    Set<WorkerTask> queryTasks = receiver.getAllTasksOfQuery(receivedMessage, 1);
    Iterator<WorkerTask> iterator = tasks.iterator();
    while (iterator.hasNext()) {
      WorkerTask task = iterator.next();
      if (queryTasks.contains(task)) {
        removeTask(iterator, task);
      }
    }
  }

  @Override
  public void run() {
    while (!isInterrupted()) {
      long currentLoad = 0;
      Iterator<WorkerTask> iterator = tasks.iterator();
      while (!isInterrupted() && iterator.hasNext()) {
        WorkerTask task = iterator.next();
        try {
          long currentLoadOfThisTask = task.getCurrentTaskLoad();
          if (task.hasInput() || task.hasToPerformFinalSteps()) {
            task.execute();
          }
          if (task.isInFinalState()) {
            removeTask(iterator, task);
          } else {
            currentLoad += currentLoadOfThisTask;
          }
        } catch (Exception e) {
          if (logger != null) {
            logger.throwing(e.getStackTrace()[0].getClassName(),
                    e.getStackTrace()[0].getMethodName(), e);
          }
          removeTask(iterator, task);
          messageSender.sendQueryTaskFailed(0, task.getCoordinatorID(), "Execution of task " + task
                  + "failed. Cause:\n" + e.getClass().getName() + ": " + e.getMessage());
        }
      }
      this.currentLoad = currentLoad;
      if (isInterrupted()) {
        messageSender.sendAllBufferedMessages(mappingCache);
      }
      rebalance();
      if (tasks.isEmpty()) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
        }
      }
    }
  }

  /**
   * <code>task = iterator.next()</code> has to be called in advance!
   * 
   * @param iterator
   * @param task
   */
  private void removeTask(Iterator<WorkerTask> iterator, WorkerTask task) {
    iterator.remove();
    receiver.unregister(task);
    task.close();
  }

  private void rebalance() {
    if (getPrevious().id < getNext().id) {
      rebalance(getNext());
      rebalance(getPrevious());
    } else {
      rebalance(getPrevious());
      rebalance(getNext());
    }
  }

  private void rebalance(WorkerThread other) {
    if (id == other.id) {
      return;
    }
    // only if this worker has more load then the other, tasks are shifted
    synchronized (id > other.id ? this : other) {
      synchronized (id > other.id ? other : this) {
        // dead locks are avoided
        long loadDiff = getCurrentLoad() - other.getCurrentLoad();
        if (loadDiff <= Math.ceil(unbalanceThreshold * getCurrentLoad())) {
          // the worker threads are balanced or this worker has less
          // work than the other
          return;
        }
        Set<WorkerTask> tasksToShift = getTasksToShift(loadDiff / 2);
        for (WorkerTask task : tasksToShift) {
          tasks.remove(task);
          currentLoad -= task.getCurrentTaskLoad();
          other.receiveTask(task);
        }
      }
    }
  }

  private Set<WorkerTask> getTasksToShift(long unbalancedLoad) {
    if ((unbalancedLoad <= 0) || !tasks.isEmpty()) {
      return new HashSet<>();
    }

    // filter out useless tasks and sort tasks according to load
    NavigableSet<WorkerTask> relevantTasks = new TreeSet<>(new WorkerTaskComparator(true));
    for (WorkerTask task : tasks) {
      long load = task.getCurrentTaskLoad();
      if ((load == 0) || (load > unbalancedLoad)) {
        continue;
      }
      relevantTasks.add(task);
    }

    // perform greedy algorithm to find minimal amount of tasks to shift to
    // the other
    Set<WorkerTask> tasksToShift = new HashSet<>();
    long remainingLoad = unbalancedLoad;
    for (WorkerTask task : relevantTasks.descendingSet()) {
      long taskLoad = task.getCurrentTaskLoad();
      if (taskLoad <= remainingLoad) {
        tasksToShift.add(task);
        remainingLoad -= taskLoad;
        if (remainingLoad == 0) {
          return tasksToShift;
        }
      }
    }
    return tasksToShift;
  }

  public void clear() {
    messageSender.clear();
    terminateTasks();
  }

  @Override
  public void close() {
    if (isAlive()) {
      interrupt();
    }
    messageSender.close(mappingCache);
    terminateTasks();
    receiver.close();
  }

  private void terminateTasks() {
    synchronized (this) {
      Iterator<WorkerTask> iter = tasks.iterator();
      while (iter.hasNext()) {
        WorkerTask task = iter.next();
        removeTask(iter, task);
      }
    }
  }

  @Override
  public String toString() {
    return tasks.toString();
  }

}
