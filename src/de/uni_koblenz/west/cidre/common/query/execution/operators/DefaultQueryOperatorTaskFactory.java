package de.uni_koblenz.west.cidre.common.query.execution.operators;

import java.io.File;

import de.uni_koblenz.west.cidre.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.cidre.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.cidre.common.query.TriplePattern;
import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorTaskFactoryBase;
import de.uni_koblenz.west.cidre.slave.triple_store.TripleStoreAccessor;

/**
 * Provides methods to create the query execution tree.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class DefaultQueryOperatorTaskFactory
		extends QueryOperatorTaskFactoryBase {

	public DefaultQueryOperatorTaskFactory(long coordinatorId,
			int numberOfSlaves, int cacheSize, File cacheDirectory) {
		super(coordinatorId, numberOfSlaves, cacheSize, cacheDirectory);
	}

	public DefaultQueryOperatorTaskFactory(
			QueryOperatorTaskFactoryBase taskFactory) {
		super(taskFactory);
	}

	@Override
	public QueryOperatorTask createTriplePatternMatch(long taskId,
			int emittedMappingsPerRound, TriplePattern pattern,
			TripleStoreAccessor tripleStore) {
		return new TriplePatternMatchOperator(taskId, coordinatorId,
				numberOfSlaves, cacheSize, cacheDirectory, pattern,
				emittedMappingsPerRound, tripleStore);
	}

	@Override
	public QueryOperatorTask createTriplePatternJoin(long taskId,
			int emittedMappingsPerRound, QueryOperatorTask leftChild,
			QueryOperatorTask rightChild, MapDBStorageOptions storageType,
			boolean useTransactions, boolean writeAsynchronously,
			MapDBCacheOptions cacheType) {
		return new TriplePatternJoinOperator(taskId, coordinatorId,
				numberOfSlaves, cacheSize, cacheDirectory,
				emittedMappingsPerRound, leftChild, rightChild, storageType,
				useTransactions, writeAsynchronously, cacheType);
	}

	@Override
	public QueryOperatorTask createProjection(long taskId,
			int emittedMappingsPerRound, long[] resultVars,
			QueryOperatorTask subOperation) {
		return new ProjectionOperator(taskId, coordinatorId, numberOfSlaves,
				cacheSize, cacheDirectory, emittedMappingsPerRound, resultVars,
				subOperation);
	}

	@Override
	public QueryOperatorTask createSlice(long taskId,
			int emittedMappingsPerRound, QueryOperatorTask subOperation,
			long offset, long length) {
		return new SliceOperator(taskId, coordinatorId, numberOfSlaves,
				cacheSize, cacheDirectory, emittedMappingsPerRound,
				subOperation, offset, length);
	}

}
