package de.uni_koblenz.west.cidre.common.query.execution;

import java.io.File;

import de.uni_koblenz.west.cidre.common.executor.WorkerTask;
import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;

/**
 * This is the base implementation of {@link WorkerTask} that is common for all
 * query operations.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public abstract class QueryOperatorBase extends QueryTaskBase
		implements QueryOperatorTask {

	private final long coordinatorId;

	private QueryOperatorBase parent;

	public QueryOperatorBase(short slaveId, int queryId, short taskId,
			long coordinatorId, long estimatedWorkLoad, int numberOfSlaves,
			int cacheSize, File cacheDirectory) {
		super((((((long) slaveId) << Integer.SIZE)
				| (queryId & 0x00_00_00_00_ff_ff_ff_ffl)) << Short.SIZE)
				| (taskId & 0x00_00_00_00_00_00_ff_ffl), estimatedWorkLoad,
				numberOfSlaves, cacheSize, cacheDirectory);
		this.coordinatorId = coordinatorId;
	}

	public QueryOperatorBase(long id, long coordinatorId,
			long estimatedWorkLoad, int numberOfSlaves, int cacheSize,
			File cacheDirectory) {
		super(id, estimatedWorkLoad, numberOfSlaves, cacheSize, cacheDirectory);
		this.coordinatorId = coordinatorId;
	}

	@Override
	public long getCoordinatorID() {
		return coordinatorId;
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
	protected void handleMappingReception(long sender, byte[] message,
			int firstIndex) {
		long taskId = (sender & 0x00_00_ff_ff_ff_ff_ff_ffl)
				| (getID() & 0xff_ff_00_00_00_00_00_00l);
		int childIndex = getIndexOfChild(taskId);
		enqueuMessage(childIndex, message, firstIndex);
	}

	@Override
	protected void executePreStartStep() {
	}

	protected void executeFinalStep(MappingRecycleCache recycleCache) {
		messageSender.sendQueryTaskFinished(getID(), getParentTask() == null,
				getCoordinatorID(), recycleCache);
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
		long joinVarValue = mapping.getValue(
				((QueryOperatorTask) getParentTask()).getFirstJoinVar())
				& 0xFF_FF_00_00_00_00_00_00l;
		return parentID | joinVarValue;
	}

	@Override
	public void close() {
		super.close();
		closeInternal();
	}

	protected abstract void closeInternal();

}
