package de.uni_koblenz.west.cidre.common.query.execution.operators.base_impl;

import java.io.File;

import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.cidre.common.query.execution.operators.ProjectionOperator;

public class ProjectionBaseOperator extends ProjectionOperator {

	private static final long serialVersionUID = 5672375567745585589L;

	public ProjectionBaseOperator(long id, long coordinatorId,
			int numberOfSlaves, int cacheSize, File cacheDirectory,
			int emittedMappingsPerRound, long[] resultVars,
			QueryOperatorTask subOperation) {
		super(id, coordinatorId, numberOfSlaves, cacheSize, cacheDirectory,
				emittedMappingsPerRound, resultVars, subOperation);
	}

	public ProjectionBaseOperator(short slaveId, int queryId, short taskId,
			long coordinatorId, int numberOfSlaves, int cacheSize,
			File cacheDirectory, int emittedMappingsPerRound, long[] resultVars,
			QueryOperatorTask subOperation) {
		super(slaveId, queryId, taskId, coordinatorId, numberOfSlaves,
				cacheSize, cacheDirectory, emittedMappingsPerRound, resultVars,
				subOperation);
	}

	@Override
	protected void emitMapping(Mapping mapping) {
		if (getParentTask() == null) {
			messageSender.sendQueryMapping(mapping, getID(), getCoordinatorID(),
					recycleCache);
		} else {
			// send to computer with smallest id
			long parentBaseID = getParentTask().getID()
					& 0x00_00_FF_FF_FF_FF_FF_FFl;
			messageSender.sendQueryMapping(mapping, getID(),
					parentBaseID | 0x00_01_00_00_00_00_00_00l, recycleCache);
		}
	}

}
