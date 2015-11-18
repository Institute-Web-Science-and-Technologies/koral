package de.uni_koblenz.west.cidre.common.query.execution;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.executor.WorkerTask;
import de.uni_koblenz.west.cidre.common.executor.messagePassing.MessageSenderBuffer;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;
import de.uni_koblenz.west.cidre.common.utils.CachedFileReceiverQueue;

/**
 * This is the base implementation of {@link WorkerTask} that is common for all
 * query operations.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public abstract class QueryOperatorBase implements WorkerTask, QueryTask {

	protected Logger logger;

	private MessageSenderBuffer messageSender;

	private MappingRecycleCache recycleCache;

	private final long id;

	private final long coordinatorId;

	private final long estimatedWorkLoad;

	private QueryOperatorState state;

	private QueryOperatorBase parent;

	private WorkerTask[] children;

	private CachedFileReceiverQueue[] inputQueues;

	private int numberOfMissingFinishedMessages;

	private final int cacheSize;

	private final File cacheDirectory;

	public QueryOperatorBase(short slaveId, int queryId, short taskId,
			long coordinatorId, long estimatedWorkLoad, int numberOfSlaves,
			int cacheSize, File cacheDirectory) {
		this((((((long) slaveId) << Integer.SIZE)
				| (queryId & 0x00_00_00_00_ff_ff_ff_ffl)) << Short.SIZE)
				| (taskId & 0x00_00_00_00_00_00_ff_ffl), coordinatorId,
				estimatedWorkLoad, numberOfSlaves, cacheSize, cacheDirectory);
	}

	public QueryOperatorBase(long id, long coordinatorId,
			long estimatedWorkLoad, int numberOfSlaves, int cacheSize,
			File cacheDirectory) {
		this.id = id;
		this.coordinatorId = coordinatorId;
		this.estimatedWorkLoad = estimatedWorkLoad;
		numberOfMissingFinishedMessages = numberOfSlaves;
		this.cacheSize = cacheSize;
		this.cacheDirectory = new File(cacheDirectory.getAbsolutePath()
				+ File.separatorChar + "operator_" + this.id);
		state = QueryOperatorState.CREATED;
	}

	@Override
	public void setUp(MessageSenderBuffer messageSender,
			MappingRecycleCache recycleCache, Logger logger) {
		this.logger = logger;
		this.messageSender = messageSender;
		this.recycleCache = recycleCache;
	}

	@Override
	public long getID() {
		return id;
	}

	@Override
	public long getCoordinatorID() {
		return coordinatorId;
	}

	@Override
	public long getEstimatedTaskLoad() {
		return estimatedWorkLoad;
	}

	public void setParentTask(WorkerTask parent) {
		if (parent == null || !(parent instanceof QueryOperatorBase)) {
			throw new IllegalArgumentException(
					"The parent worker task must be of type "
							+ getClass().getName());
		}
		this.parent = (QueryOperatorBase) parent;
	}

	@Override
	public WorkerTask getParentTask() {
		return parent;
	}

	public int addChildTask(WorkerTask child) {
		int id = 0;
		if (children == null || children.length == 0) {
			children = new WorkerTask[1];
			inputQueues = new CachedFileReceiverQueue[1];
		} else {
			WorkerTask[] newChildren = new WorkerTask[children.length + 1];
			CachedFileReceiverQueue[] newInputQueues = new CachedFileReceiverQueue[inputQueues.length
					+ 1];
			for (int i = 0; i < children.length; i++) {
				newChildren[i] = children[i];
				newInputQueues[i] = inputQueues[i];
			}
			children = newChildren;
			inputQueues = newInputQueues;
			id = children.length - 1;
		}
		children[0] = child;
		inputQueues[0] = new CachedFileReceiverQueue(cacheSize, cacheDirectory);
		return id;
	}

	@Override
	public Set<WorkerTask> getPrecedingTasks() {
		Set<WorkerTask> precedingTasks = new HashSet<>();
		for (WorkerTask child : children) {
			precedingTasks.add(child);
		}
		return precedingTasks;
	}

	@Override
	public void start() {
		if (state != QueryOperatorState.CREATED) {
			throw new IllegalStateException(
					"The query operator could not be started, becaue it is in state "
							+ state.name() + ".");
		}
		state = QueryOperatorState.STARTED;
	}

	@Override
	public boolean hasInput() {
		for (CachedFileReceiverQueue queue : inputQueues) {
			if (!queue.isEmpty()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void enqueueMessage(long sender, byte[] message, int firstIndex) {
		MessageType mType = MessageType.valueOf(message[firstIndex]);
		switch (mType) {
		case QUERY_TASK_FINISHED:
			numberOfMissingFinishedMessages--;
			break;
		case QUERY_MAPPING_BATCH:
			long taskId = (sender & 0x00_00_ff_ff_ff_ff_ff_ffl)
					| (getID() & 0xff_ff_00_00_00_00_00_00l);
			int childIndex = -1;
			for (childIndex = 0; childIndex < children.length; childIndex++) {
				if (children[childIndex].getID() == taskId) {
					break;
				}
			}
			inputQueues[childIndex].enqueue(message, firstIndex);
			break;
		default:
			throw new RuntimeException("Unsupported message type " + mType);
		}
	}

	@Override
	public void execute() {
		if (state == QueryOperatorState.STARTED) {
			executeOperationStep();
			if (isFinishedLocally()) {
				numberOfMissingFinishedMessages--;
				state = QueryOperatorState.WAITING_FOR_OTHERS_TO_FINISH;
				messageSender.sendQueryTaskFinished(getID(),
						getParentTask() == null, getCoordinatorID(),
						recycleCache);
			}
		} else if (state == QueryOperatorState.WAITING_FOR_OTHERS_TO_FINISH) {
			if (numberOfMissingFinishedMessages == 0) {
				state = QueryOperatorState.FINISHED;
			}
		}
	}

	protected abstract void executeOperationStep();

	/**
	 * Called by subclasses of {@link QueryOperatorBase}.
	 * 
	 * @param child
	 * @return the first unprocessed received {@link Mapping} of child operator
	 *         <code>child</code>. If no {@link Mapping} has been received, yet,
	 *         <code>null</code> is returned.
	 */
	protected Mapping consumeMapping(int child) {
		return inputQueues[child].dequeue(recycleCache);
	}

	/**
	 * Called by subclasses of {@link QueryOperatorBase}.
	 * 
	 * @param child
	 * @return <code>true</code> if all {@link Mapping}s of <code>child</code>
	 *         have been processed and the child operation is finished.
	 */
	protected boolean hasChildFinished(int child) {
		return inputQueues[child].isEmpty() && children[child].hasFinished();
	}

	/**
	 * Called by subclasses of {@link QueryOperatorBase}.<br>
	 * Sends <code>mapping</code> to the {@link #parent} operator. If this is
	 * the root operator, it sends the mapping to the query coordinator.
	 * 
	 * @param mapping
	 */
	protected void emitMapping(Mapping mapping) {
		messageSender.sendQueryMapping(mapping, getID(), getParentTask() == null
				? getCoordinatorID() : adjustOwner(mapping), recycleCache);
	}

	private long adjustOwner(Mapping mapping) {
		assert getParentTask() != null;
		long parentID = getParentTask().getID() & 0x00_00_FF_FF_FF_FF_FF_FFl;
		long joinVarValue = mapping
				.getValue(((QueryTask) getParentTask()).getFirstJoinVar())
				& 0xFF_FF_00_00_00_00_00_00l;
		return parentID | joinVarValue;
	}

	private boolean isFinishedLocally() {
		for (WorkerTask child : children) {
			if (!child.hasFinished()) {
				return false;
			}
		}
		return numberOfMissingFinishedMessages == 0 && isFinishedInternal();
	}

	/**
	 * @return true, if the current subclass has nothing to do any more (the
	 *         input queues and finish notifications are already checked in
	 *         {@link QueryOperatorBase}).
	 */
	protected abstract boolean isFinishedInternal();

	@Override
	public boolean hasFinished() {
		return state == QueryOperatorState.FINISHED;
	}

	@Override
	public void close() {
		for (CachedFileReceiverQueue queue : inputQueues) {
			queue.close();
		}
		if (state != QueryOperatorState.FINISHED) {
			state = QueryOperatorState.ABORTED;
		}
		closeInternal();
	}

	protected abstract void closeInternal();

	@Override
	public String toString() {
		return getClass().getName() + "[id=" + id + "(slave="
				+ (id >>> (Integer.SIZE + Short.SIZE)) + " query="
				+ ((id << Short.SIZE) >>> (Short.SIZE + Short.SIZE)) + " task="
				+ ((id << (Short.SIZE + Integer.SIZE)) >>> (Short.SIZE
						+ Integer.SIZE))
				+ ")]";
	}

}
