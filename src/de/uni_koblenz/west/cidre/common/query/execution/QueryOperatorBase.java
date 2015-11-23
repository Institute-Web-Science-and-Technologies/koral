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
			int firstIndex, int length) {
		long taskId = (sender & 0x00_00_ff_ff_ff_ff_ff_ffl)
				| (getID() & 0xff_ff_00_00_00_00_00_00l);
		int childIndex = getIndexOfChild(taskId);
		enqueuMessageInternal(childIndex, message, firstIndex, length);
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
	 * the root operator, it sends the mapping to the query coordinator. See
	 * definition of target' function (definition 31).
	 * 
	 * @param mapping
	 */
	protected void emitMapping(Mapping mapping) {
		if (getParentTask() == null) {
			messageSender.sendQueryMapping(mapping, getID(), getCoordinatorID(),
					recycleCache);
		} else {
			short thisComputerID = (short) (getID() >>> Short.BYTES
					+ Integer.BYTES);
			long parentBaseID = getParentTask().getID()
					& 0x00_00_FF_FF_FF_FF_FF_FFl;
			if (mapping.isEmptyMapping()) {
				if (mapping
						.getIdOfFirstComputerKnowingThisMapping() == thisComputerID) {
					// the first computer who knows this empty mapping, forwards
					// it to all parent tasks
					messageSender.sendQueryMappingToAll(mapping, getID(),
							parentBaseID, recycleCache);
				}
			} else {
				long ownerLong = mapping
						.getValue(((QueryOperatorTask) getParentTask())
								.getFirstJoinVar())
						& 0xFF_FF_00_00_00_00_00_00l;
				int owner = (int) (ownerLong >>> (Short.BYTES + Integer.BYTES));
				if (mapping.isKnownByComputer(owner)) {
					if (mapping
							.isKnownByComputer((int) (getID() >>> (Short.BYTES
									+ Integer.BYTES)))) {
						// the owner also knows a replicate of this mapping,
						// forward it to parent task on this computer
						messageSender.sendQueryMapping(mapping, getID(),
								getParentTask().getID(), recycleCache);
					}
				} else {
					if (mapping
							.getIdOfFirstComputerKnowingThisMapping() == thisComputerID) {
						// first knowing computer sends mapping to owner which
						// is a remote computer
						messageSender.sendQueryMapping(mapping, getID(),
								parentBaseID | ownerLong, recycleCache);
					}
				}
			}
		}
	}

	@Override
	public void close() {
		super.close();
		closeInternal();
	}

	protected abstract void closeInternal();

}
