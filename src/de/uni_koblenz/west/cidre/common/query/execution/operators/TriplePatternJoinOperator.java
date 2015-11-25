package de.uni_koblenz.west.cidre.common.query.execution.operators;

import java.io.File;
import java.util.Arrays;

import de.uni_koblenz.west.cidre.common.query.Mapping;
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

	private final JoinType joinType;

	private final UnlimitedMappingHashSet leftHashSet;

	private final UnlimitedMappingHashSet rightHashSet;

	private JoinIterator iterator;

	public TriplePatternJoinOperator(long id, long coordinatorId,
			long estimatedWorkLoad, int numberOfSlaves, int cacheSize,
			File cacheDirectory, int emittedMappingsPerRound,
			QueryOperatorBase leftChild, QueryOperatorBase rightChild,
			int numberOfHashBuckets, int maxInMemoryMappings) {
		super(id, coordinatorId, estimatedWorkLoad, numberOfSlaves, cacheSize,
				cacheDirectory, emittedMappingsPerRound);
		addChildTask(leftChild);
		addChildTask(rightChild);
		computeVars(leftChild.getResultVariables(),
				rightChild.getResultVariables());

		if (joinVars.length > 0) {
			joinType = JoinType.JOIN;
		} else {
			if (leftChild.getResultVariables().length == 0) {
				joinType = JoinType.RIGHT_FORWARD;
			} else if (rightChild.getResultVariables().length == 0) {
				joinType = JoinType.LEFT_FORWARD;
			} else {
				joinType = JoinType.CARTESIAN_PRODUCT;
			}
		}

		int numberOfInMemoryMappingsPerSet = maxInMemoryMappings / 2;
		if (numberOfInMemoryMappingsPerSet <= 0) {
			numberOfInMemoryMappingsPerSet = 1;
		}
		leftHashSet = new UnlimitedMappingHashSet(numberOfHashBuckets,
				numberOfHashBuckets, cacheDirectory, recycleCache,
				getClass().getSimpleName() + getID() + "_leftChild_");
		rightHashSet = new UnlimitedMappingHashSet(numberOfHashBuckets,
				numberOfHashBuckets, cacheDirectory, recycleCache,
				getClass().getSimpleName() + getID() + "_rightChild_");
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
		computeVars(leftChild.getResultVariables(),
				rightChild.getResultVariables());

		if (joinVars.length > 0) {
			joinType = JoinType.JOIN;
		} else {
			if (leftChild.getResultVariables().length == 0) {
				joinType = JoinType.RIGHT_FORWARD;
			} else if (rightChild.getResultVariables().length == 0) {
				joinType = JoinType.LEFT_FORWARD;
			} else {
				joinType = JoinType.CARTESIAN_PRODUCT;
			}
		}

		int numberOfInMemoryMappingsPerSet = maxInMemoryMappings / 2;
		if (numberOfInMemoryMappingsPerSet <= 0) {
			numberOfInMemoryMappingsPerSet = 1;
		}
		leftHashSet = new UnlimitedMappingHashSet(numberOfHashBuckets,
				numberOfHashBuckets, cacheDirectory, recycleCache,
				getClass().getSimpleName() + getID() + "_leftChild_");
		rightHashSet = new UnlimitedMappingHashSet(numberOfHashBuckets,
				numberOfHashBuckets, cacheDirectory, recycleCache,
				getClass().getSimpleName() + getID() + "_rightChild_");
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
		long leftSize = getSizeOfInputQueue(0) + leftHashSet.size();
		long rightSize = getSizeOfInputQueue(1) + rightHashSet.size();
		if (leftSize == 0) {
			return rightSize;
		} else if (rightSize == 0) {
			return leftSize;
		} else {
			return leftSize * rightSize;
		}
	}

	@Override
	protected void executeOperationStep() {
		switch (joinType) {
		case JOIN:
		case CARTESIAN_PRODUCT:
			executeJoinStep();
			break;
		case LEFT_FORWARD:
			executeLeftForwardStep();
			break;
		case RIGHT_FORWARD:
			executeRightForwardStep();
			break;
		}
	}

	private void executeJoinStep() {
		for (int i = 0; i < getEmittedMappingsPerRound(); i++) {
			if (iterator == null || !iterator.hasNext()) {
				if (shouldConsumefromLeftChild()) {
					if (isInputQueueEmpty(0)) {
						if (hasChildFinished(0)) {
							// left child is finished
							rightHashSet.close();
						}
						if (isInputQueueEmpty(1)) {
							// there are no mappings to consume
							return;
						}
					} else {
						Mapping mapping = consumeMapping(0);
						leftHashSet.add(mapping, getFirstJoinVar());
						iterator = new JoinIterator(recycleCache, joinVars,
								mapping,
								((QueryOperatorBase) getChildTask(0))
										.getResultVariables(),
								joinType == JoinType.CARTESIAN_PRODUCT
										? rightHashSet.iterator()
										: rightHashSet.getMatchCandidates(
												mapping, getFirstJoinVar()),
								((QueryOperatorBase) getChildTask(1))
										.getResultVariables());
					}
				} else {
					if (isInputQueueEmpty(1)) {
						if (hasChildFinished(1)) {
							// right child is finished
							leftHashSet.close();
						}
						if (isInputQueueEmpty(0)) {
							// there are no mappings to consume
							return;
						}
					} else {
						Mapping mapping = consumeMapping(1);
						rightHashSet.add(mapping, getFirstJoinVar());
						iterator = new JoinIterator(recycleCache, joinVars,
								mapping,
								((QueryOperatorBase) getChildTask(1))
										.getResultVariables(),
								joinType == JoinType.CARTESIAN_PRODUCT
										? rightHashSet.iterator()
										: rightHashSet.getMatchCandidates(
												mapping, getFirstJoinVar()),
								((QueryOperatorBase) getChildTask(0))
										.getResultVariables());
					}
				}
				i--;
			} else {
				emitMapping(iterator.next());
			}
		}
	}

	private boolean shouldConsumefromLeftChild() {
		if (isInputQueueEmpty(1)) {
			return true;
		} else if (isInputQueueEmpty(0)) {
			return false;
		} else {
			return leftHashSet.size() < rightHashSet.size();
		}
	}

	private void executeLeftForwardStep() {
		if (rightHashSet.isEmpty()) {
			if (hasChildFinished(1)) {
				// the right child has finished successfully
				Mapping mapping = consumeMapping(1);
				if (mapping != null) {
					rightHashSet.add(mapping, getFirstJoinVar());
				}
			} else {
				// no match for the right expression could be found
				// discard all mappings received from left child
				while (!isInputQueueEmpty(0)) {
					Mapping mapping = consumeMapping(0);
					recycleCache.releaseMapping(mapping);
				}
			}
		} else {
			// the right child has matched
			for (int i = 0; i < getEmittedMappingsPerRound()
					&& !isInputQueueEmpty(0); i++) {
				emitMapping(consumeMapping(0));
			}
		}
	}

	private void executeRightForwardStep() {
		if (leftHashSet.isEmpty()) {
			if (hasChildFinished(0)) {
				// the left child has finished successfully
				Mapping mapping = consumeMapping(0);
				if (mapping != null) {
					leftHashSet.add(mapping, getFirstJoinVar());
				}
			} else {
				// no match for the left expression could be found
				// discard all mappings received from right child
				while (!isInputQueueEmpty(1)) {
					Mapping mapping = consumeMapping(1);
					recycleCache.releaseMapping(mapping);
				}
			}
		} else {
			// the left child has matched
			for (int i = 0; i < getEmittedMappingsPerRound()
					&& !isInputQueueEmpty(1); i++) {
				emitMapping(consumeMapping(1));
			}
		}
	}

	@Override
	protected boolean isFinishedInternal() {
		return true;
	}

	@Override
	protected void executeFinalStep() {
	}

	@Override
	protected void closeInternal() {
		leftHashSet.close();
		rightHashSet.close();
	}

	private static enum JoinType {
		JOIN, CARTESIAN_PRODUCT, LEFT_FORWARD, RIGHT_FORWARD;
	}
}
