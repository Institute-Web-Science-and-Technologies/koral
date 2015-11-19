package de.uni_koblenz.west.cidre.common.query.execution;

import java.io.File;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.executor.WorkerTask;
import de.uni_koblenz.west.cidre.common.executor.WorkerTaskBase;
import de.uni_koblenz.west.cidre.common.executor.messagePassing.MessageSenderBuffer;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;

/**
 * This is the base implementation of {@link WorkerTask} that is common for all
 * query operations.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public abstract class QueryOperatorBase extends WorkerTaskBase
		implements QueryOperatorTask {

	private MessageSenderBuffer messageSender;

	private MappingRecycleCache recycleCache;

	private final long coordinatorId;

	private final long estimatedWorkLoad;

	private QueryOperatorBase parent;

	private QueryOperatorState state;

	private int numberOfMissingFinishedMessages;

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
		super(id, cacheSize, cacheDirectory);
		this.coordinatorId = coordinatorId;
		this.estimatedWorkLoad = estimatedWorkLoad;
		numberOfMissingFinishedMessages = numberOfSlaves;
		state = QueryOperatorState.CREATED;
	}

	@Override
	public void setUp(MessageSenderBuffer messageSender,
			MappingRecycleCache recycleCache, Logger logger) {
		super.setUp(messageSender, recycleCache, logger);
		this.messageSender = messageSender;
		this.recycleCache = recycleCache;
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
	public void enqueueMessage(long sender, byte[] message, int firstIndex) {
		MessageType mType = MessageType.valueOf(message[firstIndex]);
		switch (mType) {
		case QUERY_TASK_FINISHED:
			numberOfMissingFinishedMessages--;
			break;
		case QUERY_MAPPING_BATCH:
			long taskId = (sender & 0x00_00_ff_ff_ff_ff_ff_ffl)
					| (getID() & 0xff_ff_00_00_00_00_00_00l);
			int childIndex = getIndexOfChild(taskId);
			enqueuMessage(childIndex, message, firstIndex);
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
		return consumeMapping(child, recycleCache);
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
				.getValue(((QueryOperatorTask) getParentTask()).getFirstJoinVar())
				& 0xFF_FF_00_00_00_00_00_00l;
		return parentID | joinVarValue;
	}

	private boolean isFinishedLocally() {
		return numberOfMissingFinishedMessages == 0 && areAllChildrenFinished()
				&& isFinishedInternal();
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
		super.close();
		if (state != QueryOperatorState.FINISHED) {
			state = QueryOperatorState.ABORTED;
		}
		closeInternal();
	}

	protected abstract void closeInternal();

}
