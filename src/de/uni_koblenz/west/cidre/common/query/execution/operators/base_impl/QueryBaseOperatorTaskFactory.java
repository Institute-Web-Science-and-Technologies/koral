package de.uni_koblenz.west.cidre.common.query.execution.operators.base_impl;

import java.io.File;

import de.uni_koblenz.west.cidre.common.query.TriplePattern;
import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.cidre.common.query.execution.operators.QueryOperatorTaskFactory;
import de.uni_koblenz.west.cidre.slave.triple_store.TripleStoreAccessor;

public class QueryBaseOperatorTaskFactory extends QueryOperatorTaskFactory {

	public QueryBaseOperatorTaskFactory(short slaveId, int queryId,
			long coordinatorId, int numberOfSlaves, int cacheSize,
			File cacheDirectory) {
		super(slaveId, queryId, coordinatorId, numberOfSlaves, cacheSize,
				cacheDirectory);
	}

	@Override
	public QueryOperatorTask createProjection(int emittedMappingsPerRound,
			long[] resultVars, QueryOperatorTask subOperation) {
		return new ProjectionBaseOperator(slaveId, emittedMappingsPerRound,
				getNextTaskId(), emittedMappingsPerRound,
				emittedMappingsPerRound, emittedMappingsPerRound,
				cacheDirectory, emittedMappingsPerRound, resultVars,
				subOperation);
	}

	@Override
	public QueryOperatorTask createTriplePatternJoin(
			int emittedMappingsPerRound, QueryOperatorTask leftChild,
			QueryOperatorTask rightChild, int numberOfHashBuckets,
			int maxInMemoryMappings) {
		return new TriplePatternJoinBaseOperator(slaveId, maxInMemoryMappings,
				getNextTaskId(), maxInMemoryMappings, maxInMemoryMappings,
				maxInMemoryMappings, cacheDirectory, emittedMappingsPerRound,
				leftChild, rightChild, numberOfHashBuckets,
				maxInMemoryMappings);
	}

	@Override
	public QueryOperatorTask createTriplePatternMatch(
			int emittedMappingsPerRound, TriplePattern pattern,
			TripleStoreAccessor tripleStore) {
		return new TriplePatternMatchBaseOperator(slaveId,
				emittedMappingsPerRound, getNextTaskId(),
				emittedMappingsPerRound, emittedMappingsPerRound,
				emittedMappingsPerRound, cacheDirectory, pattern,
				emittedMappingsPerRound, tripleStore);
	}

}
