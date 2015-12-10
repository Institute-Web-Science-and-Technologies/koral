package de.uni_koblenz.west.cidre.common.query.execution.operators.base_impl;

import java.io.File;

import de.uni_koblenz.west.cidre.common.query.TriplePattern;
import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorTaskFactoryBase;
import de.uni_koblenz.west.cidre.slave.triple_store.TripleStoreAccessor;

public class QueryBaseOperatorTaskFactory extends QueryOperatorTaskFactoryBase {

	public QueryBaseOperatorTaskFactory(long coordinatorId, int numberOfSlaves,
			int cacheSize, File cacheDirectory) {
		super(coordinatorId, numberOfSlaves, cacheSize, cacheDirectory);
	}

	public QueryBaseOperatorTaskFactory(
			QueryOperatorTaskFactoryBase taskFactory) {
		super(taskFactory);
	}

	@Override
	public QueryOperatorTask createTriplePatternMatch(long taskId,
			int emittedMappingsPerRound, TriplePattern pattern,
			TripleStoreAccessor tripleStore) {
		return new TriplePatternMatchBaseOperator(taskId, coordinatorId,
				numberOfSlaves, cacheSize, cacheDirectory, pattern,
				emittedMappingsPerRound, tripleStore);
	}

	@Override
	public QueryOperatorTask createTriplePatternJoin(long taskId,
			int emittedMappingsPerRound, QueryOperatorTask leftChild,
			QueryOperatorTask rightChild, int numberOfHashBuckets,
			int maxInMemoryMappings) {
		return new TriplePatternJoinBaseOperator(taskId, coordinatorId,
				numberOfSlaves, cacheSize, cacheDirectory,
				emittedMappingsPerRound, leftChild, rightChild,
				numberOfHashBuckets, maxInMemoryMappings);
	}

	@Override
	public QueryOperatorTask createProjection(long taskId,
			int emittedMappingsPerRound, long[] resultVars,
			QueryOperatorTask subOperation) {
		return new ProjectionBaseOperator(taskId, coordinatorId, numberOfSlaves,
				cacheSize, cacheDirectory, emittedMappingsPerRound, resultVars,
				subOperation);
	}

}
