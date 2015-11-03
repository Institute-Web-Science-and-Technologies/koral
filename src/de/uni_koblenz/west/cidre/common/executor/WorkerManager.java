package de.uni_koblenz.west.cidre.common.executor;

import java.io.Closeable;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;

public class WorkerManager implements Closeable, AutoCloseable {

	private final Logger logger;

	private final WorkerThread[] executors;

	public WorkerManager(Configuration conf, Logger logger) {
		this.logger = logger;
		int availableCPUs = Runtime.getRuntime().availableProcessors() - 1;
		if (availableCPUs < 1) {
			availableCPUs = 1;
		}
		executors = new WorkerThread[availableCPUs];
		for (int i = 0; i < executors.length; i++) {
			// TODO handle message notification
			executors[i] = new WorkerThread(i,
					conf.getSizeOfMappingRecycleCache(),
					conf.getUnbalanceThresholdForWorkerThreads(), null, logger);
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
				new WorkerTaskComparator());
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
				new WorkerTaskComparator());
		for (WorkerTask task : workingSet) {
			newWorkingSet.addAll(task.gerPrecedingTasks());
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

	@Override
	public void close() {
		for (WorkerThread executor : executors) {
			if (executor != null) {
				executor.close();
			}
		}
	}

}
