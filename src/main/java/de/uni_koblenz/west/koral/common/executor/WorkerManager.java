/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License along with Koral. If not,
 * see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
package de.uni_koblenz.west.koral.common.executor;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.logging.Logger;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.executor.messagePassing.MessageReceiverListener;
import de.uni_koblenz.west.koral.common.executor.messagePassing.MessageSender;
import de.uni_koblenz.west.koral.common.executor.messagePassing.MessageSenderBuffer;
import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.measurement.MeasurementType;
import de.uni_koblenz.west.koral.common.messages.MessageNotifier;
import de.uni_koblenz.west.koral.common.query.execution.QueryExecutionTreeDeserializer;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.slave.triple_store.TripleStoreAccessor;

/**
 * This class manages the different {@link WorkerThread}s, i.e., starting and stopping the threads
 * as well as starting and stopping the {@link WorkerTask} of a query. When a new query is started
 * it is also responsible for the initial scheduling of the corresponding {@link WorkerTask}s among
 * all {@link WorkerThread}s.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class WorkerManager implements Closeable, AutoCloseable {

  private final Logger logger;

  private final MeasurementCollector measurementCollector;

  private final MessageNotifier messageNotifier;

  private final MessageSenderBuffer messageSender;

  private final MessageReceiverListener messageReceiver;

  private final WorkerThread[] workers;

  private final int numberOfSlaves;

  private TripleStoreAccessor tripleStore;

  private final int cacheSize;

  private final File cacheDirectory;

  private final MapDBStorageOptions storageType;

  private final boolean useTransactions;

  private final boolean writeAsynchronously;

  private final MapDBCacheOptions cacheType;

  public WorkerManager(Configuration conf, MessageNotifier notifier, MessageSender messageSender,
      boolean flagIsMaster, Logger logger, MeasurementCollector measurementCollector) {
    this(conf, null, notifier, messageSender, flagIsMaster, logger, measurementCollector);
  }

  public WorkerManager(Configuration conf, TripleStoreAccessor tripleStore,
      MessageNotifier notifier, MessageSender messageSender, boolean flagIsMaster, Logger logger,
      MeasurementCollector measurementCollector) {
    this.logger = logger;
    this.measurementCollector = measurementCollector;
    messageNotifier = notifier;
    messageReceiver = new MessageReceiverListener(logger);
    this.messageSender = new MessageSenderBuffer(conf.getNumberOfSlaves(),
        conf.getMappingBundleSize(), messageSender, messageReceiver, logger, measurementCollector);
    messageNotifier.registerMessageListener(messageReceiver.getClass(), messageReceiver);
    numberOfSlaves = conf.getNumberOfSlaves();
    this.tripleStore = tripleStore;
    cacheSize = conf.getReceiverQueueSize();
    cacheDirectory = new File(conf.getTmpDirByInstance(flagIsMaster));
    cacheType = conf.getJoinCacheType();
    storageType = conf.getJoinCacheStorageType();
    useTransactions = conf.useTransactionsForJoinCache();
    writeAsynchronously = conf.isJoinCacheAsynchronouslyWritten();

    int availableCPUs = Runtime.getRuntime().availableProcessors() - 1;
    if (availableCPUs < 1) {
      availableCPUs = 1;
    }
    workers = new WorkerThread[availableCPUs];
    for (int i = 0; i < workers.length; i++) {
      workers[i] = new WorkerThread(i, conf.getSizeOfMappingRecycleCache(),
          conf.getUnbalanceThresholdForWorkerThreads(), messageReceiver, this.messageSender,
          numberOfSlaves, logger, measurementCollector);
      if (i > 0) {
        workers[i - 1].setNext(workers[i]);
        workers[i].setPrevious(workers[i - 1]);
      }
    }
    workers[workers.length - 1].setNext(workers[0]);
    workers[0].setPrevious(workers[workers.length - 1]);
    if (this.logger != null) {
      this.logger.info(availableCPUs + " executor threads started");
    }
  }

  public void setTripleStore(TripleStoreAccessor tripleStore) {
    this.tripleStore = tripleStore;
  }

  public void addTask(WorkerTask rootTask) {
    initializeTaskTree(rootTask);
  }

  public void createQuery(byte[] receivedQUERY_CREATEMessage) {
    int computerOfQueryExecutionCoordinator =
        NumberConversion.bytes2short(receivedQUERY_CREATEMessage, Byte.BYTES + Integer.BYTES + 1)
            & 0x00_00_ff_ff;
    long coordinatorId =
        NumberConversion.bytes2long(receivedQUERY_CREATEMessage, Byte.BYTES + Integer.BYTES + 1);
    int queryId = (int) ((coordinatorId & 0x00_00_ff_ff_ff_ff_00_00L) >>> Short.SIZE);
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.QUERY_SLAVE_QUERY_CREATION_START,
          System.currentTimeMillis(), Integer.toString(queryId));
    }
    QueryExecutionTreeDeserializer deserializer =
        new QueryExecutionTreeDeserializer(tripleStore, numberOfSlaves, cacheSize, cacheDirectory,
            storageType, useTransactions, writeAsynchronously, cacheType);
    try (DataInputStream input = new DataInputStream(
        new ByteArrayInputStream(receivedQUERY_CREATEMessage, Byte.BYTES + Integer.BYTES,
            receivedQUERY_CREATEMessage.length - Byte.BYTES - Integer.BYTES));) {
      QueryOperatorTask queryExecutionTree = deserializer.deserialize(input);
      initializeTaskTree(queryExecutionTree);
      messageSender.sendQueryCreated(computerOfQueryExecutionCoordinator, coordinatorId);
      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.QUERY_SLAVE_QUERY_CREATION_END,
            System.currentTimeMillis(), Integer.toString(queryId));
      }
      if (logger != null) {
        logger.finer("Query " + queryId + " created.");
      }
    } catch (Throwable e) {
      String message = "Error during deserialization of query "
          + NumberConversion.bytes2int(receivedQUERY_CREATEMessage, Byte.BYTES) + ".";
      if (logger != null) {
        logger.finer(message);
        logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
            e);
      }
      messageSender.sendQueryTaskFailed(computerOfQueryExecutionCoordinator, coordinatorId,
          message + " Cause: " + e.getMessage());
    }
  }

  private void initializeTaskTree(WorkerTask rootTask) {
    // initialize current work load of WorkerThreads
    long[] workLoad = new long[workers.length];
    for (int i = 0; i < workers.length; i++) {
      workLoad[i] = workers[i].getCurrentLoad();
    }
    NavigableSet<WorkerTask> workingSet = new TreeSet<>(new WorkerTaskComparator(true));
    workingSet.add(rootTask);
    assignTasks(workLoad, workingSet);
  }

  private void assignTasks(long[] estimatedWorkLoad, NavigableSet<WorkerTask> workingSet) {
    if (workingSet.isEmpty()) {
      return;
    }
    // process children first! i.e. proceeding tasks are finished before
    // their succeeding tasks.
    NavigableSet<WorkerTask> newWorkingSet = new TreeSet<>(new WorkerTaskComparator(true));
    for (WorkerTask task : workingSet) {
      newWorkingSet.addAll(task.getPrecedingTasks());
    }
    assignTasks(estimatedWorkLoad, newWorkingSet);
    // now assign current tasks to WorkerThreads
    for (WorkerTask task : workingSet.descendingSet()) {
      int workerWithMinimalWorkload = findMinimal(estimatedWorkLoad);
      try {
        workers[workerWithMinimalWorkload].addWorkerTask(task);
      } catch (IllegalThreadStateException e) {
        if (logger != null) {
          logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
              e);
        }
        workers[workerWithMinimalWorkload] = new WorkerThread(workers[workerWithMinimalWorkload]);
        workers[workerWithMinimalWorkload].addWorkerTask(task);
      }
      estimatedWorkLoad[workerWithMinimalWorkload] += task.getEstimatedTaskLoad();
    }
  }

  private int findMinimal(long[] estimatedWorkLoad) {
    long minimalValue = Long.MAX_VALUE;
    int currentMin = -1;
    for (int i = 0; i < estimatedWorkLoad.length; i++) {
      if (estimatedWorkLoad[i] < minimalValue) {
        currentMin = i;
        minimalValue = estimatedWorkLoad[i];
      }
    }
    return currentMin;
  }

  public void startQuery(byte[] receivedMessage) {
    // startQuery() on any worker will start all QueryTasks independent to
    // which worker it is assigned.
    if ((workers != null) && (workers.length > 0)) {
      workers[0].startQuery(receivedMessage);
    }
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.QUERY_SLAVE_QUERY_EXECUTION_START,
          System.currentTimeMillis(),
          Integer.toString(NumberConversion.bytes2int(receivedMessage, 1)));
    }
    if (logger != null) {
      logger.finer("Query " + NumberConversion.bytes2int(receivedMessage, 1) + " started.");
    }
  }

  public void abortQuery(byte[] receivedMessage) {
    for (WorkerThread worker : workers) {
      worker.abortQuery(receivedMessage);
    }
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.QUERY_SLAVE_QUERY_EXECUTION_ABORT,
          System.currentTimeMillis(),
          Integer.toString(NumberConversion.bytes2int(receivedMessage, 1)));
    }
    if (logger != null) {
      logger.finer("Query " + NumberConversion.bytes2int(receivedMessage, 1) + " aborted.");
    }
  }

  public void clear() {
    for (WorkerThread executor : workers) {
      if (executor != null) {
        executor.clear();
      }
    }
    messageNotifier.registerMessageListener(messageReceiver.getClass(), messageReceiver);
  }

  @Override
  public void close() {
    MessageReceiverListener receiver = null;
    for (WorkerThread executor : workers) {
      if (executor != null) {
        executor.close();
        receiver = executor.getReceiver();
      }
    }
    if (receiver != null) {
      messageNotifier.unregisterMessageListener(receiver.getClass(), receiver);
    }
  }

}
