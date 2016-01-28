package de.uni_koblenz.west.cidre.common.query.execution;

import java.io.File;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.executor.WorkerTaskBase;
import de.uni_koblenz.west.cidre.common.executor.messagePassing.MessageSenderBuffer;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;

/**
 * Common superclass for query cordinator and all query operations.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public abstract class QueryTaskBase extends WorkerTaskBase {

	protected MappingRecycleCache recycleCache;

	protected MessageSenderBuffer messageSender;

	private long estimatedWorkLoad;

	private QueryTaskState state;

	private int numberOfMissingFinishedMessages;

	public QueryTaskBase(short slaveId, int queryId, short taskId,
			int numberOfSlaves, int cacheSize, File cacheDirectory) {
		this((((((long) slaveId) << Integer.SIZE)
				| (queryId & 0x00_00_00_00_ff_ff_ff_ffl)) << Short.SIZE)
				| (taskId & 0x00_00_00_00_00_00_ff_ffl), numberOfSlaves,
				cacheSize, cacheDirectory);
	}

	public QueryTaskBase(long id, int numberOfSlaves, int cacheSize,
			File cacheDirectory) {
		super(id, cacheSize, cacheDirectory);
		numberOfMissingFinishedMessages = numberOfSlaves;
		state = QueryTaskState.CREATED;
	}

	@Override
	public void setUp(MessageSenderBuffer messageSender,
			MappingRecycleCache recycleCache, Logger logger) {
		super.setUp(messageSender, recycleCache, logger);
		this.recycleCache = recycleCache;
		this.messageSender = messageSender;
	}

	public void setEstimatedWorkLoad(long estimatedWorkLoad) {
		this.estimatedWorkLoad = estimatedWorkLoad;
	}

	@Override
	public long getEstimatedTaskLoad() {
		return estimatedWorkLoad;
	}

	@Override
	public void start() {
		// TODO removed
		if (logger != null) {
			logger.info(
					"current state of task " + getID() + " is " + state.name());
		}
		if (state != QueryTaskState.CREATED) {
			throw new IllegalStateException(
					"The query task could not be started, because it is in state "
							+ state.name() + ".");
		}
		state = QueryTaskState.STARTED;
	}

	@Override
	public void enqueueMessage(long sender, byte[] message, int firstIndex,
			int messageLength) {
		MessageType mType = MessageType.valueOf(message[firstIndex]);
		switch (mType) {
		case QUERY_TASK_FINISHED:
			numberOfMissingFinishedMessages--;
			handleFinishNotification(sender, message, firstIndex,
					messageLength);
			break;
		case QUERY_MAPPING_BATCH:
			handleMappingReception(sender, message, firstIndex, messageLength);
			break;
		default:
			throw new RuntimeException("Unsupported message type " + mType);
		}
	}

	protected abstract void handleFinishNotification(long sender, Object object,
			int firstIndex, int messageLength);

	protected abstract void handleMappingReception(long sender, byte[] message,
			int firstIndex, int length);

	@Override
	public void execute() {
		if (state == QueryTaskState.CREATED) {
			executePreStartStep();
		} else if (state == QueryTaskState.STARTED) {
			executeOperationStep();
			if (isFinishedLocally()) {
				numberOfMissingFinishedMessages--;
				state = QueryTaskState.WAITING_FOR_OTHERS_TO_FINISH;
				executeFinalStep();
			}
		} else if (state == QueryTaskState.WAITING_FOR_OTHERS_TO_FINISH) {
			if (numberOfMissingFinishedMessages == 0) {
				state = QueryTaskState.FINISHED;
			}
		}
	}

	protected abstract void executePreStartStep();

	protected abstract void executeOperationStep();

	protected abstract void executeFinalStep();

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
		return hasFinishedSuccessfully() || isAborted();
	}

	public boolean hasFinishedSuccessfully() {
		return state == QueryTaskState.FINISHED;
	}

	public boolean isAborted() {
		return state == QueryTaskState.ABORTED;
	}

	@Override
	public void close() {
		super.close();
		if (state != QueryTaskState.FINISHED) {
			state = QueryTaskState.ABORTED;
		}
	}

}
