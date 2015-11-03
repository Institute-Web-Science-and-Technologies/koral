package de.uni_koblenz.west.cidre.common.executor;

import java.io.Closeable;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.networManager.MessageNotifier;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;

public class WorkerThread extends Thread implements Closeable, AutoCloseable {

	private final Logger logger;

	private final int id;

	private final MessageNotifier messageNotifier;

	private final MappingRecycleCache mappingCache;

	private WorkerThread previous;

	private WorkerThread next;

	private final double unbalanceThreshold;

	private final ConcurrentLinkedQueue<WorkerTask> tasks;

	private long currentLoad;

	public WorkerThread(int id, int sizeOfMappingRecycleCache,
			double unbalanceThreshold, MessageNotifier messageNotifier,
			Logger logger) {
		this.logger = logger;
		this.id = id;
		setName("WorkerThread " + id);
		tasks = new ConcurrentLinkedQueue<>();
		currentLoad = 0;
		this.messageNotifier = messageNotifier;
		mappingCache = new MappingRecycleCache(sizeOfMappingRecycleCache);
		this.unbalanceThreshold = unbalanceThreshold;
		// TODO Auto-generated constructor stub
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

	public void addWorkerTask(WorkerTask task) {
		boolean wasEmpty = tasks.isEmpty();
		tasks.offer(task);
		task.setUp(mappingCache, logger);
		// TODO handle messagePassing
		if (wasEmpty) {
			start();
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
					if (task.hasInput()) {
						task.execute();
					}
					if (task.hasFinished()) {
						// TODO remove message handling
						iterator.remove();
						task.close();
					} else {
						currentLoad += task.getCurrentTaskLoad();
					}
				} catch (Exception e) {
					if (logger != null) {
						logger.throwing(e.getStackTrace()[0].getClassName(),
								e.getStackTrace()[0].getMethodName(), e);
					}
					// TODO remove message handling
					iterator.remove();
					task.close();
					// TODO handle failed query processing
				}
			}
			this.currentLoad = currentLoad;
			rebalance();
			if (tasks.isEmpty()) {
				try {
					sleep(100);
				} catch (InterruptedException e) {
				}
			}
		}
	}

	private void rebalance() {
		if (previous.id < next.id) {
			rebalance(next);
			rebalance(previous);
		} else {
			rebalance(previous);
			rebalance(next);
		}
	}

	private void rebalance(WorkerThread other) {
		if (id == other.id) {
			return;
		}
		synchronized (id > other.id ? this : other) {
			synchronized (id > other.id ? other : this) {
				// dead locks are avoided
			}
		}
		// TODO Auto-generated method stub

	}

	public void clear() {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
