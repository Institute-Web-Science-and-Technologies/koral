package de.uni_koblenz.west.cidre.common.executor;

import java.io.Closeable;
import java.util.Set;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;
import de.uni_koblenz.west.cidre.common.query.messagePassing.MessageSenderBuffer;

public interface WorkerTask extends Closeable {

	public boolean setUp(MessageSenderBuffer messageSender,
			MappingRecycleCache recycleCache, Logger logger);

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
	 * @return id of the root node (i.e., the query coordinator node) of the
	 *         query execution tree
	 */
	public long getRootID();

	public long getEstimatedTaskLoad();

	public long getCurrentTaskLoad();

	public WorkerTask getParentTask();

	public Set<WorkerTask> getPrecedingTasks();

	/**
	 * Starts to emit results on the next {@link #execute()} call.
	 */
	public void start();

	public boolean hasInput();

	public void enqueueMessage(byte[] message);

	/**
	 * Results may only be emitted, it {@link #start()} was called.
	 */
	public void execute();

	public boolean hasFinished();

	@Override
	public void close();

	@Override
	public String toString();

}
