package de.uni_koblenz.west.cidre.common.query.execution.operators;

import java.io.File;

import de.uni_koblenz.west.cidre.common.query.TriplePattern;
import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.cidre.slave.triple_store.TripleStoreAccessor;

/**
 * Provides methods to create the query execution tree.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class QueryOperatorTaskFactory {

	private final short slaveId;

	private final int queryId;

	private int nextTaskId;

	private final long coordinatorId;

	private final int numberOfSlaves;

	private final int cacheSize;

	private final File cacheDirectory;

	public QueryOperatorTaskFactory(short slaveId, int queryId,
			long coordinatorId, int numberOfSlaves, int cacheSize,
			File cacheDirectory) {
		this.slaveId = slaveId;
		this.queryId = queryId;
		nextTaskId = 0;
		this.coordinatorId = coordinatorId;
		this.numberOfSlaves = numberOfSlaves;
		this.cacheSize = cacheSize;
		this.cacheDirectory = cacheDirectory;
	}

	private short getNextTaskId() {
		if (nextTaskId > (Short.MAX_VALUE) - Short.MIN_VALUE) {
			throw new RuntimeException(
					"The maximal number of tasks have already been created.");
		}
		return (short) nextTaskId++;
	}

	public QueryOperatorTask createTriplePatternMatch(
			int emittedMappingsPerRound, TriplePattern pattern,
			TripleStoreAccessor tripleStore) {
		return new TriplePatternMatchOperator(slaveId, queryId, getNextTaskId(),
				coordinatorId, numberOfSlaves, cacheSize, cacheDirectory,
				pattern, emittedMappingsPerRound, tripleStore);
	}

	public QueryOperatorTask createTriplePatternJoin(
			int emittedMappingsPerRound, QueryOperatorTask leftChild,
			QueryOperatorTask rightChild, int numberOfHashBuckets,
			int maxInMemoryMappings) {
		return new TriplePatternJoinOperator(slaveId, queryId, getNextTaskId(),
				coordinatorId, numberOfSlaves, cacheSize, cacheDirectory,
				emittedMappingsPerRound, leftChild, rightChild,
				numberOfHashBuckets, maxInMemoryMappings);
	}

	public QueryOperatorTask createProjection(int emittedMappingsPerRound,
			long[] resultVars, QueryOperatorTask subOperation) {
		return new ProjectionOperator(slaveId, queryId, getNextTaskId(),
				coordinatorId, numberOfSlaves, cacheSize, cacheDirectory,
				emittedMappingsPerRound, resultVars, subOperation);
	}

}
