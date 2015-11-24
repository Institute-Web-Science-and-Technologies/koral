package de.uni_koblenz.west.cidre.common.query.execution.operators;

import java.io.File;
import java.util.Arrays;

import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorBase;
import de.uni_koblenz.west.cidre.common.utils.UnlimitedMappingCache;

public class TriplePatternJoinOperator extends QueryOperatorBase {

	private long[] resultVars;

	private long[] joinVars;

	private final int numberOfHashBuckets;

	private final int numberOfInMemoryMappingsPerCache;

	private final UnlimitedMappingCache[][] joinVarsIndex2leftHashCache;

	private final UnlimitedMappingCache[][] joinVarsIndex2rightHashCache;

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
		numberOfInMemoryMappingsPerCache = number;
		joinVarsIndex2leftHashCache = new UnlimitedMappingCache[joinVars.length][];
		joinVarsIndex2rightHashCache = new UnlimitedMappingCache[joinVars.length][];
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
		int number = maxInMemoryMappings
				/ (joinVars.length * this.numberOfHashBuckets);
		if (number <= 0) {
			number = 1;
		}
		numberOfInMemoryMappingsPerCache = number;
		joinVarsIndex2leftHashCache = new UnlimitedMappingCache[joinVars.length][];
		joinVarsIndex2rightHashCache = new UnlimitedMappingCache[joinVars.length][];
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
	protected void handleFinishNotification(long sender, Object object,
			int firstIndex, int messageLength) {
		// TODO Auto-generated method stub
		super.handleFinishNotification(sender, object, firstIndex,
				messageLength);
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
		// TODO Auto-generated method stub

	}

}
