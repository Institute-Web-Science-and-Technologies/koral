package de.uni_koblenz.west.cidre.common.query.execution.operators;

import java.io.File;

import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorBase;

public class ProjectionOperator extends QueryOperatorBase {

	private final long[] resultVars;

	public ProjectionOperator(long id, long coordinatorId,
			long estimatedWorkLoad, int numberOfSlaves, int cacheSize,
			File cacheDirectory, int emittedMappingsPerRound, long[] resultVars,
			QueryOperatorBase subOperation) {
		super(id, coordinatorId, estimatedWorkLoad, numberOfSlaves, cacheSize,
				cacheDirectory, emittedMappingsPerRound);
		this.resultVars = resultVars;
		addChildTask(subOperation);
	}

	public ProjectionOperator(short slaveId, int queryId, short taskId,
			long coordinatorId, long estimatedWorkLoad, int numberOfSlaves,
			int cacheSize, File cacheDirectory, int emittedMappingsPerRound,
			long[] resultVars, QueryOperatorBase subOperation) {
		super(slaveId, queryId, taskId, coordinatorId, estimatedWorkLoad,
				numberOfSlaves, cacheSize, cacheDirectory,
				emittedMappingsPerRound);
		this.resultVars = resultVars;
		addChildTask(subOperation);
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
				mapping = recycleCache
						.getMappingWithRestrictedVariables(mapping, resultVars);
				emitMapping(mapping);
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

}
