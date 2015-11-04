package de.uni_koblenz.west.cidre.common.executor;

import java.io.Closeable;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.networManager.MessageNotifier;
import de.uni_koblenz.west.cidre.common.query.messagePassing.MessageReceiverListener;

public class WorkerManager implements Closeable, AutoCloseable {

	private final Logger logger;

	private final MessageNotifier messageNotifier;

	private final WorkerThread[] executors;

	// TODO query ids and query task ids have to start with 0! reuse query ids!

	@SuppressWarnings("unchecked")
	public WorkerManager(Configuration conf, MessageNotifier notifier,
			Logger logger) {
		this.logger = logger;
		messageNotifier = notifier;
		MessageReceiverListener receiver = new MessageReceiverListener(logger);
		notifier.registerMessageListener(
				(Class<MessageReceiverListener>) receiver.getClass(), receiver);
		int availableCPUs = Runtime.getRuntime().availableProcessors() - 1;
		if (availableCPUs < 1) {
			availableCPUs = 1;
		}
		executors = new WorkerThread[availableCPUs];
		for (int i = 0; i < executors.length; i++) {
			// TODO handle message notification
			executors[i] = new WorkerThread(i,
					conf.getSizeOfMappingRecycleCache(),
					conf.getUnbalanceThresholdForWorkerThreads(), receiver,
					logger);
			if (i > 0) {
				executors[i - 1].setNext(executors[i]);
				executors[i].setPrevious(executors[i - 1]);
			}
		}
		executors[executors.length - 1].setNext(executors[0]);
		executors[0].setPrevious(executors[executors.length - 1]);
		if (this.logger != null) {
			this.logger.info(availableCPUs + " executor threads started");
		}
	}

	public void startTaskTree(WorkerTask rootTask) {
		// initialize current work load of WorkerThreads
		long[] workLoad = new long[executors.length];
		for (int i = 0; i < executors.length; i++) {
			workLoad[i] = executors[i].getCurrentLoad();
		}
		NavigableSet<WorkerTask> workingSet = new TreeSet<>(
				new WorkerTaskComparator(true));
		workingSet.add(rootTask);
		assignTasks(workLoad, workingSet);
	}

	private void assignTasks(long[] estimatedWorkLoad,
			NavigableSet<WorkerTask> workingSet) {
		if (workingSet.isEmpty()) {
			return;
		}
		// process children first! i.e. proceeding tasks are finished before
		// their succeeding tasks.
		NavigableSet<WorkerTask> newWorkingSet = new TreeSet<>(
				new WorkerTaskComparator(true));
		for (WorkerTask task : workingSet) {
			newWorkingSet.addAll(task.getPrecedingTasks());
		}
		assignTasks(estimatedWorkLoad, newWorkingSet);
		// now assign current tasks to WorkerThreads
		for (WorkerTask task : workingSet.descendingSet()) {
			int workerWithMinimalWorkload = findMinimal(estimatedWorkLoad);
			executors[workerWithMinimalWorkload].addWorkerTask(task);
			estimatedWorkLoad[workerWithMinimalWorkload] += task
					.getEstimatedTaskLoad();
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

	public void clear() {
		for (WorkerThread executor : executors) {
			if (executor != null) {
				executor.clear();
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void close() {
		MessageReceiverListener receiver = null;
		for (WorkerThread executor : executors) {
			if (executor != null) {
				executor.close();
				receiver = executor.getReceiver();
			}
		}
		if (receiver != null) {
			messageNotifier.unregisterMessageListener(
					(Class<MessageReceiverListener>) receiver.getClass(),
					receiver);
		}
	}

}
