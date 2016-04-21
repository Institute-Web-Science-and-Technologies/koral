package de.uni_koblenz.west.koral.common.executor;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.executor.messagePassing.MessageReceiverListener;
import de.uni_koblenz.west.koral.common.executor.messagePassing.MessageSender;
import de.uni_koblenz.west.koral.common.executor.messagePassing.MessageSenderBuffer;
import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.messages.MessageNotifier;
import de.uni_koblenz.west.koral.common.query.execution.QueryExecutionTreeDeserializer;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.slave.triple_store.TripleStoreAccessor;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.logging.Logger;

/**
 * This class manages the different {@link WorkerThread}s, i.e., starting and
 * stopping the threads as well as starting and stopping the {@link WorkerTask}
 * of a query. When a new query is started it is also responsible for the
 * initial scheduling of the corresponding {@link WorkerTask}s among all
 * {@link WorkerThread}s.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class WorkerManager implements Closeable, AutoCloseable {

  private final Logger logger;

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
          Logger logger) {
    this(conf, null, notifier, messageSender, logger);
  }

  public WorkerManager(Configuration conf, TripleStoreAccessor tripleStore,
          MessageNotifier notifier, MessageSender messageSender, Logger logger) {
    this.logger = logger;
    messageNotifier = notifier;
    messageReceiver = new MessageReceiverListener(logger);
    this.messageSender = new MessageSenderBuffer(conf.getNumberOfSlaves(),
            conf.getMappingBundleSize(), messageSender, messageReceiver, logger);
    messageNotifier.registerMessageListener(messageReceiver.getClass(), messageReceiver);
    numberOfSlaves = conf.getNumberOfSlaves();
    this.tripleStore = tripleStore;
    cacheSize = conf.getReceiverQueueSize();
    cacheDirectory = new File(conf.getTmpDir());
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
              numberOfSlaves, logger);
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
    int computerOfQueryExecutionCoordinator = NumberConversion.bytes2short(
            receivedQUERY_CREATEMessage, Byte.BYTES + Integer.BYTES + 1) & 0x00_00_ff_ff;
    long coordinatorId = NumberConversion.bytes2long(receivedQUERY_CREATEMessage,
            Byte.BYTES + Integer.BYTES + 1);
    QueryExecutionTreeDeserializer deserializer = new QueryExecutionTreeDeserializer(tripleStore,
            numberOfSlaves, cacheSize, cacheDirectory, storageType, useTransactions,
            writeAsynchronously, cacheType);
    try (DataInputStream input = new DataInputStream(
            new ByteArrayInputStream(receivedQUERY_CREATEMessage, Byte.BYTES + Integer.BYTES,
                    receivedQUERY_CREATEMessage.length - Byte.BYTES - Integer.BYTES));) {
      QueryOperatorTask queryExecutionTree = deserializer.deserialize(input);
      initializeTaskTree(queryExecutionTree);
      messageSender.sendQueryCreated(computerOfQueryExecutionCoordinator, coordinatorId);
      if (logger != null) {
        int queryID = (int) ((queryExecutionTree.getID() >>> Short.SIZE)
                & 0x00_00_00_00_ff_ff_ff_ffl);
        logger.finer("Query " + queryID + " created.");
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
      workers[workerWithMinimalWorkload].addWorkerTask(task);
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
    if (workers != null && workers.length > 0) {
      workers[0].startQuery(receivedMessage);
    }
    if (logger != null) {
      logger.finer("Query " + NumberConversion.bytes2int(receivedMessage, 1) + " started.");
    }
  }

  public void abortQuery(byte[] receivedMessage) {
    for (WorkerThread worker : workers) {
      worker.abortQuery(receivedMessage);
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
