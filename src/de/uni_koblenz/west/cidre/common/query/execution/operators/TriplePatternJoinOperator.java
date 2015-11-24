package de.uni_koblenz.west.cidre.common.query.execution.operators;

import java.io.File;
import java.util.Arrays;

import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorBase;
import de.uni_koblenz.west.cidre.common.utils.UnlimitedMappingHashSet;

/**
 * Performs the join operation of mappings as a hash join.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class TriplePatternJoinOperator extends QueryOperatorBase {

	private long[] resultVars;

	private long[] joinVars;

	private final int numberOfHashBuckets;

	private final int numberOfInMemoryMappingsPerSet;

	private final UnlimitedMappingHashSet[] joinVarsIndex2leftHashSet;

	private final UnlimitedMappingHashSet[] joinVarsIndex2rightHashSet;

	public TriplePatternJoinOperator(long id, long coordinatorId,
			long estimatedWorkLoad, int numberOfSlaves, int cacheSize,
			File cacheDirectory, int emittedMappingsPerRound,
			QueryOperatorBase leftChild, QueryOperatorBase rightChild,
			int numberOfHashBuckets, int maxInMemoryMappings) {
		super(id, coordinatorId, estimatedWorkLoad, numberOfSlaves, cacheSize,
				cacheDirectory, emittedMappingsPerRound);
		addChildTask(leftChild);
		addChildTask(rightChild);
		this.numberOfHashBuckets = numberOfHashBuckets;
		computeVars(leftChild.getResultVariables(),
				rightChild.getResultVariables());
		int number = maxInMemoryMappings
				/ (joinVars.length * this.numberOfHashBuckets);
		if (number <= 0) {
			number = 1;
		}
		numberOfInMemoryMappingsPerSet = number;
		joinVarsIndex2leftHashSet = new UnlimitedMappingHashSet[joinVars.length];
		joinVarsIndex2rightHashSet = new UnlimitedMappingHashSet[joinVars.length];
	}

	public TriplePatternJoinOperator(short slaveId, int queryId, short taskId,
			long coordinatorId, long estimatedWorkLoad, int numberOfSlaves,
			int cacheSize, File cacheDirectory, int emittedMappingsPerRound,
			QueryOperatorBase leftChild, QueryOperatorBase rightChild,
			int numberOfHashBuckets, int maxInMemoryMappings) {
		super(slaveId, queryId, taskId, coordinatorId, estimatedWorkLoad,
				numberOfSlaves, cacheSize, cacheDirectory,
				emittedMappingsPerRound);
		addChildTask(leftChild);
		addChildTask(rightChild);
		this.numberOfHashBuckets = numberOfHashBuckets;
		computeVars(leftChild.getResultVariables(),
				rightChild.getResultVariables());
		int number = maxInMemoryMappings / joinVars.length;
		if (number <= 0) {
			number = 1;
		}
		numberOfInMemoryMappingsPerSet = number;
		joinVarsIndex2leftHashSet = new UnlimitedMappingHashSet[joinVars.length];
		joinVarsIndex2rightHashSet = new UnlimitedMappingHashSet[joinVars.length];
	}

	private void computeVars(long[] leftVars, long[] rightVars) {
		long[] allVars = new long[leftVars.length + rightVars.length];
		System.arraycopy(leftVars, 0, allVars, 0, leftVars.length);
		System.arraycopy(rightVars, 0, allVars, leftVars.length,
				rightVars.length);
		Arrays.sort(allVars);
		// count occurrences of different variable types
		int numberOfJoinVars = 0;
		int numberOfResultVars = 0;
		for (int i = 0; i < allVars.length; i++) {
			if (i > 0 && allVars[i - 1] == allVars[i]) {
				// each variable occurs at most two times
				numberOfJoinVars++;
			} else {
				numberOfResultVars++;
			}
		}
		// assign variables to arrays
		resultVars = new long[numberOfResultVars];
		joinVars = new long[numberOfJoinVars];
		for (int i = 0; i < allVars.length; i++) {
			if (i > 0 && allVars[i - 1] == allVars[i]) {
				// each variable occurs at most two times
				joinVars[i] = allVars[i];
			} else {
				resultVars[i] = allVars[i];
			}
		}
	}

	// TODO handle cartesian product

	@Override
	public long[] getResultVariables() {
		return resultVars;
	}

	@Override
	public long getFirstJoinVar() {
		return joinVars.length == 0 ? getResultVariables()[0] : joinVars[0];
	}

	@Override
	public long getCurrentTaskLoad() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	protected void executeOperationStep() {
		// TODO Auto-generated method stub

	}

	@Override
	protected boolean isFinishedInternal() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected void executeFinalStep() {
		// TODO Auto-generated method stub

	}

	@Override
	protected void closeInternal() {
		closeCaches(joinVarsIndex2leftHashSet);
		closeCaches(joinVarsIndex2rightHashSet);
	}

	private void closeCaches(UnlimitedMappingHashSet[] sets) {
		for (UnlimitedMappingHashSet set : sets) {
			if (set != null) {
				set.close();
			}
		}
	}
}
