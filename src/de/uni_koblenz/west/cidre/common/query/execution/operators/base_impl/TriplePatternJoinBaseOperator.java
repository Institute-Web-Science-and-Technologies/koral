package de.uni_koblenz.west.cidre.common.query.execution.operators.base_impl;

import java.io.File;

import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.cidre.common.query.execution.operators.TriplePatternJoinOperator;

public class TriplePatternJoinBaseOperator extends TriplePatternJoinOperator {

	public TriplePatternJoinBaseOperator(long id, long coordinatorId,
			int numberOfSlaves, int cacheSize, File cacheDirectory,
			int emittedMappingsPerRound, QueryOperatorTask leftChild,
			QueryOperatorTask rightChild, int numberOfHashBuckets,
			int maxInMemoryMappings) {
		super(id, coordinatorId, numberOfSlaves, cacheSize, cacheDirectory,
				emittedMappingsPerRound, leftChild, rightChild,
				numberOfHashBuckets, maxInMemoryMappings);
	}

	public TriplePatternJoinBaseOperator(short slaveId, int queryId,
			short taskId, long coordinatorId, int numberOfSlaves, int cacheSize,
			File cacheDirectory, int emittedMappingsPerRound,
			QueryOperatorTask leftChild, QueryOperatorTask rightChild,
			int numberOfHashBuckets, int maxInMemoryMappings) {
		super(slaveId, queryId, taskId, coordinatorId, numberOfSlaves,
				cacheSize, cacheDirectory, emittedMappingsPerRound, leftChild,
				rightChild, numberOfHashBuckets, maxInMemoryMappings);
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
