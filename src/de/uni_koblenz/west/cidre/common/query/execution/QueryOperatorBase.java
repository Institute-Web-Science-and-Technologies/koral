package de.uni_koblenz.west.cidre.common.query.execution;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import de.uni_koblenz.west.cidre.common.executor.WorkerTask;
import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.execution.operators.ProjectionOperator;
import de.uni_koblenz.west.cidre.common.utils.NumberConversion;
import de.uni_koblenz.west.cidre.master.statisticsDB.GraphStatistics;

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

	private final int emittedMappingsPerRound;

	public QueryOperatorBase(short slaveId, int queryId, short taskId,
			long coordinatorId, int numberOfSlaves, int cacheSize,
			File cacheDirectory, int emittedMappingsPerRound) {
		super((((((long) slaveId) << Integer.SIZE)
				| (queryId & 0x00_00_00_00_ff_ff_ff_ffl)) << Short.SIZE)
				| (taskId & 0x00_00_00_00_00_00_ff_ffl), numberOfSlaves,
				cacheSize, cacheDirectory);
		this.coordinatorId = coordinatorId;
		this.emittedMappingsPerRound = emittedMappingsPerRound;
	}

	public QueryOperatorBase(long id, long coordinatorId, int numberOfSlaves,
			int cacheSize, File cacheDirectory, int emittedMappingsPerRound) {
		super(id, numberOfSlaves, cacheSize, cacheDirectory);
		this.coordinatorId = coordinatorId;
		this.emittedMappingsPerRound = emittedMappingsPerRound;
	}

	/**
	 * @param statistics
	 * @param slave
	 *            the first slave has id 0
	 * @return
	 */
	public long computeEstimatedLoad(GraphStatistics statistics, int slave) {
		return computeEstimatedLoad(statistics, slave, false);
	}

	public abstract long computeEstimatedLoad(GraphStatistics statistics,
			int slave, boolean setLoads);

	public abstract long computeTotalEstimatedLoad(GraphStatistics statistics);

	public void adjustEstimatedLoad(GraphStatistics statistics, int slave) {
		computeEstimatedLoad(statistics, slave, true);
	}

	@Override
	public long getCoordinatorID() {
		return coordinatorId;
	}

	protected int getEmittedMappingsPerRound() {
		return emittedMappingsPerRound;
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
	protected void handleFinishNotification(long sender, Object object,
			int firstIndex, int messageLength) {
	}

	@Override
	protected void handleMappingReception(long sender, byte[] message,
			int firstIndex, int length) {
		long taskId = (sender & 0x00_00_ff_ff_ff_ff_ff_ffl)
				| (getID() & 0xff_ff_00_00_00_00_00_00l);
		int childIndex = getIndexOfChild(taskId);
		enqueuMessage(childIndex, message, firstIndex, length);
	}

	@Override
	public boolean hasInput() {
		if (getChildTask(0) == null) {
			return !isFinishedLocally();
		} else {
			return super.hasInput();
		}
	}

	@Override
	protected boolean isFinishedLocally() {
		// prevent infinite loop if leefs in query execution tree do not change
		// implementation of this method
		return !super.hasInput();
	}

	@Override
	public boolean hasToPerformFinalSteps() {
		if (getChildTask(0) == null) {
			return isFinishedLocally() || getEstimatedTaskLoad() == 0;
		} else {
			return super.hasToPerformFinalSteps();
		}
	}

	@Override
	protected void executePreStartStep() {
	}

	@Override
	protected void executeFinalStep() {
		messageSender.sendQueryTaskFinished(getID(), getParentTask() == null,
				getCoordinatorID(), recycleCache);
	}

	@Override
	protected void tidyUp() {
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
		if (logger != null) {
			// TODO remove
			logger.info(NumberConversion.id2description(getID())
					+ " emitted mapping\n"
					+ mapping.toString(getResultVariables()));
		}
		if (getParentTask() == null) {
			messageSender.sendQueryMapping(mapping, getID(), getCoordinatorID(),
					recycleCache);
		} else if (getParentTask() instanceof ProjectionOperator) {
			// projection operator filters all mappings on the same computer
			messageSender.sendQueryMapping(mapping, getID(),
					getParentTask().getID(), recycleCache);
		} else {
			short thisComputerID = (short) (getID() >>> (Short.SIZE
					+ Integer.SIZE));
			long parentBaseID = getParentTask().getID()
					& 0x00_00_FF_FF_FF_FF_FF_FFl;
			if (mapping.isEmptyMapping()) {
				if (mapping
						.getIdOfFirstComputerKnowingThisMapping() == thisComputerID) {
					// the first computer who knows this empty mapping, forwards
					// it to all parent tasks
					mapping.setContainmentToAll();
					messageSender.sendQueryMappingToAll(mapping, getID(),
							parentBaseID, recycleCache);
				}
			} else {
				long firstJoinVar = ((QueryOperatorTask) getParentTask())
						.getFirstJoinVar();
				if (firstJoinVar == -1) {
					// parent task has no join variables
					// send to computer with smallest id
					messageSender.sendQueryMapping(mapping, getID(),
							parentBaseID | 0x00_01_00_00_00_00_00_00l,
							recycleCache);
				} else {
					long ownerLong = mapping.getValue(firstJoinVar,
							getResultVariables()) & 0xFF_FF_00_00_00_00_00_00l;
					int owner = ((int) (ownerLong >>> (Short.SIZE
							+ Integer.SIZE))) + 1;
					ownerLong = ((long) owner) << (Integer.SIZE + Short.SIZE);
					if (mapping.isKnownByComputer(owner)) {
						if (mapping.isKnownByComputer(
								(int) (getID() >>> (Short.SIZE
										+ Integer.SIZE)))) {
							// the owner also knows a replicate of this mapping,
							// forward it to parent task on this computer
							messageSender.sendQueryMapping(mapping, getID(),
									getParentTask().getID(), recycleCache);
						}
					} else {
						if (mapping
								.getIdOfFirstComputerKnowingThisMapping() == thisComputerID) {
							// first knowing computer sends mapping to owner
							// which is a remote computer
							mapping.updateContainment(
									(int) (getID() >>> (Short.SIZE
											+ Integer.SIZE)),
									owner);
							messageSender.sendQueryMapping(mapping, getID(),
									parentBaseID | ownerLong, recycleCache);
						}
					}
				}
			}
		}
	}

	@Override
	public byte[] serialize(boolean useBaseImplementation, int slaveId) {
		ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
		try (DataOutputStream output = new DataOutputStream(byteOutput);) {
			serialize(output, useBaseImplementation, slaveId);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return byteOutput.toByteArray();
	}

	protected long getIdOnSlave(int slaveId) {
		return ((getID() << Short.SIZE) >>> Short.SIZE)
				| (((long) slaveId) << (Integer.SIZE + Short.SIZE));
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		toString(sb, 0);
		return sb.toString();
	}

	public abstract void toString(StringBuilder sb, int indention);

	protected void indent(StringBuilder sb, int indention) {
		for (int i = 0; i < indention; i++) {
			sb.append("    ");
		}
	}

	@Override
	public void close() {
		super.close();
		closeInternal();
	}

	protected abstract void closeInternal();

}
