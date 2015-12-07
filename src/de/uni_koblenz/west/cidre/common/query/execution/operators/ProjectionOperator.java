package de.uni_koblenz.west.cidre.common.query.execution.operators;

import java.io.File;

import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorBase;
import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.cidre.master.statisticsDB.GraphStatistics;

/**
 * Performs the projection operation.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class ProjectionOperator extends QueryOperatorBase {

	private final long[] resultVars;

	public ProjectionOperator(long id, long coordinatorId, int numberOfSlaves,
			int cacheSize, File cacheDirectory, int emittedMappingsPerRound,
			long[] resultVars, QueryOperatorTask subOperation) {
		super(id, coordinatorId, numberOfSlaves, cacheSize, cacheDirectory,
				emittedMappingsPerRound);
		this.resultVars = resultVars;
		addChildTask(subOperation);
	}

	public ProjectionOperator(short slaveId, int queryId, short taskId,
			long coordinatorId, int numberOfSlaves, int cacheSize,
			File cacheDirectory, int emittedMappingsPerRound, long[] resultVars,
			QueryOperatorTask subOperation) {
		super(slaveId, queryId, taskId, coordinatorId, numberOfSlaves,
				cacheSize, cacheDirectory, emittedMappingsPerRound);
		this.resultVars = resultVars;
		addChildTask(subOperation);
	}

	@Override
	public long computeEstimatedLoad(GraphStatistics statistics, int slave,
			boolean setLoads) {
		QueryOperatorBase subOp = (QueryOperatorBase) getChildTask(0);
		long load = subOp.computeEstimatedLoad(statistics, slave, setLoads);
		if (setLoads) {
			setEstimatedWorkLoad(load);
		}
		return load;
	}

	@Override
	public long computeTotalEstimatedLoad(GraphStatistics statistics) {
		QueryOperatorBase subOp = (QueryOperatorBase) getChildTask(0);
		return subOp.computeTotalEstimatedLoad(statistics);
	}

	@Override
	public long[] getResultVariables() {
		return resultVars;
	}

	@Override
	public long getFirstJoinVar() {
		long min = Long.MAX_VALUE;
		for (long var : resultVars) {
			if (var < min) {
				min = var;
			}
		}
		return min;
	}

	@Override
	public long getCurrentTaskLoad() {
		return getSizeOfInputQueue(0);
	}

	@Override
	protected void closeInternal() {
	}

	@Override
	protected void executeOperationStep() {
		for (int i = 0; i < getEmittedMappingsPerRound()
				&& !isInputQueueEmpty(0); i++) {
			Mapping mapping = consumeMapping(0);
			if (mapping != null) {
				Mapping result = recycleCache.getMappingWithRestrictedVariables(
						mapping, ((QueryOperatorBase) getChildTask(0))
								.getResultVariables(),
						resultVars);
				emitMapping(result);
				recycleCache.releaseMapping(mapping);
			}
		}
	}

	@Override
	protected void executeFinalStep() {
	}

	@Override
	protected boolean isFinishedInternal() {
		return true;
	}

	@Override
	public void toString(StringBuilder sb, int indention) {
		indent(sb, indention);
		sb.append(getClass().getSimpleName());
		sb.append(" resultVars: [");
		String delim = "";
		for (long var : getResultVariables()) {
			sb.append(delim).append("?").append(var);
			delim = ",";
		}
		sb.append("]");
		sb.append(" estimatedWorkLoad: ").append(getEstimatedTaskLoad());
		sb.append("\n");
		((QueryOperatorBase) getChildTask(0)).toString(sb, indention + 1);
	}

}
