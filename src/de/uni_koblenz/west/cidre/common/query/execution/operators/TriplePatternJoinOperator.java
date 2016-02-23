package de.uni_koblenz.west.cidre.common.query.execution.operators;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.executor.messagePassing.MessageSenderBuffer;
import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;
import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorBase;
import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorType;
import de.uni_koblenz.west.cidre.common.utils.UnlimitedMappingHashSet;
import de.uni_koblenz.west.cidre.master.statisticsDB.GraphStatistics;

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

	private final int numberOfHashBuckets;

	private UnlimitedMappingHashSet leftHashSet;

	private UnlimitedMappingHashSet rightHashSet;

	private JoinIterator iterator;

	public TriplePatternJoinOperator(long id, long coordinatorId,
			int numberOfSlaves, int cacheSize, File cacheDirectory,
			int emittedMappingsPerRound, QueryOperatorTask leftChild,
			QueryOperatorTask rightChild, int numberOfHashBuckets,
			int maxInMemoryMappings) {
		super(id, coordinatorId, numberOfSlaves, cacheSize, cacheDirectory,
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

		this.numberOfHashBuckets = numberOfHashBuckets;
	}

	public TriplePatternJoinOperator(short slaveId, int queryId, short taskId,
			long coordinatorId, int numberOfSlaves, int cacheSize,
			File cacheDirectory, int emittedMappingsPerRound,
			QueryOperatorTask leftChild, QueryOperatorTask rightChild,
			int numberOfHashBuckets, int maxInMemoryMappings) {
		super(slaveId, queryId, taskId, coordinatorId, numberOfSlaves,
				cacheSize, cacheDirectory, emittedMappingsPerRound);
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

		this.numberOfHashBuckets = numberOfHashBuckets;
	}

	@Override
	public void setUp(MessageSenderBuffer messageSender,
			MappingRecycleCache recycleCache, Logger logger) {
		super.setUp(messageSender, recycleCache, logger);
		leftHashSet = new UnlimitedMappingHashSet(numberOfHashBuckets,
				numberOfHashBuckets, getCacheDirectory(), recycleCache,
				getClass().getSimpleName() + getID() + "_leftChild_");
		rightHashSet = new UnlimitedMappingHashSet(numberOfHashBuckets,
				numberOfHashBuckets, getCacheDirectory(), recycleCache,
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
		int nextJoinVarIndex = 0;
		for (int i = 0; i < allVars.length; i++) {
			if (i > 0 && allVars[i - 1] == allVars[i]) {
				// each variable occurs at most two times
				joinVars[nextJoinVarIndex] = allVars[i];
				nextJoinVarIndex++;
			} else {
				resultVars[i - nextJoinVarIndex] = allVars[i];
			}
		}
	}

	@Override
	public long computeEstimatedLoad(GraphStatistics statistics, int slave,
			boolean setLoads) {
		long load = 0;
		long totalLoad = statistics.getTotalOwnerLoad();
		if (totalLoad != 0) {
			double loadFactor = ((double) statistics.getOwnerLoad(slave))
					/ totalLoad;
			if (loadFactor != 0) {
				long joinSize = computeTotalEstimatedLoad(statistics);
				if (joinSize != 0) {
					load = (long) (joinSize * loadFactor);
				}
			}
		}
		if (setLoads) {
			((QueryOperatorBase) getChildTask(0))
					.computeEstimatedLoad(statistics, slave, setLoads);
			((QueryOperatorBase) getChildTask(1))
					.computeEstimatedLoad(statistics, slave, setLoads);
			setEstimatedWorkLoad(load);
		}
		return load;
	}

	@Override
	public long computeTotalEstimatedLoad(GraphStatistics statistics) {
		QueryOperatorBase leftChild = (QueryOperatorBase) getChildTask(0);
		long leftLoad = leftChild.computeTotalEstimatedLoad(statistics);
		if (leftLoad == 0) {
			return 0;
		}
		QueryOperatorBase rightChild = (QueryOperatorBase) getChildTask(1);
		long rightLoad = rightChild.computeTotalEstimatedLoad(statistics);
		if (rightLoad == 0) {
			return 0;
		}
		return leftLoad * rightLoad;
	}

	@Override
	public long[] getResultVariables() {
		return resultVars;
	}

	@Override
	public long getFirstJoinVar() {
		return joinVars.length == 0 ? -1 : joinVars[0];
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
						long[] mappingVars = ((QueryOperatorBase) getChildTask(
								0)).getResultVariables();
						long[] rightVars = ((QueryOperatorBase) getChildTask(1))
								.getResultVariables();
						leftHashSet.add(mapping, getFirstJoinVar(),
								mappingVars);
						iterator = new JoinIterator(recycleCache,
								getResultVariables(), joinVars, mapping,
								mappingVars,
								joinType == JoinType.CARTESIAN_PRODUCT
										? rightHashSet.iterator()
										: rightHashSet.getMatchCandidates(
												mapping, getFirstJoinVar(),
												mappingVars),
								rightVars);
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
						long[] mappingVars = ((QueryOperatorBase) getChildTask(
								1)).getResultVariables();
						long[] leftVars = ((QueryOperatorBase) getChildTask(0))
								.getResultVariables();
						rightHashSet.add(mapping, getFirstJoinVar(),
								mappingVars);
						iterator = new JoinIterator(recycleCache,
								getResultVariables(), joinVars, mapping,
								mappingVars,
								joinType == JoinType.CARTESIAN_PRODUCT
										? leftHashSet.iterator()
										: leftHashSet.getMatchCandidates(
												mapping, getFirstJoinVar(),
												mappingVars),
								leftVars);
					}
				}
				i--;
			} else {
				Mapping resultMapping = iterator.next();
				emitMapping(resultMapping);
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
					long[] rightVars = ((QueryOperatorBase) getChildTask(1))
							.getResultVariables();
					rightHashSet.add(mapping, getFirstJoinVar(), rightVars);
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
					long[] leftVars = ((QueryOperatorBase) getChildTask(0))
							.getResultVariables();
					leftHashSet.add(mapping, getFirstJoinVar(), leftVars);
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
	protected void closeInternal() {
		leftHashSet.close();
		rightHashSet.close();
	}

	@Override
	public void serialize(DataOutputStream output,
			boolean useBaseImplementation, int slaveId) throws IOException {
		if (getParentTask() == null) {
			output.writeBoolean(useBaseImplementation);
			output.writeLong(getCoordinatorID());
		}
		output.writeInt(QueryOperatorType.TRIPLE_PATTERN_JOIN.ordinal());
		((QueryOperatorTask) getChildTask(0)).serialize(output,
				useBaseImplementation, slaveId);
		((QueryOperatorTask) getChildTask(1)).serialize(output,
				useBaseImplementation, slaveId);
		output.writeLong(getIdOnSlave(slaveId));
		output.writeInt(getEmittedMappingsPerRound());
		output.writeLong(getEstimatedTaskLoad());
	}

	@Override
	public void toString(StringBuilder sb, int indention) {
		indent(sb, indention);
		sb.append(getClass().getSimpleName());
		sb.append(" ").append(joinType.name());
		sb.append(" joinVars: [");
		String delim = "";
		for (long var : joinVars) {
			sb.append(delim).append(var);
			delim = ",";
		}
		sb.append("]");
		sb.append(" resultVars: [");
		delim = "";
		for (long var : resultVars) {
			sb.append(delim).append(var);
			delim = ",";
		}
		sb.append("]");
		sb.append(" estimatedWorkLoad: ").append(getEstimatedTaskLoad());
		sb.append("\n");
		((QueryOperatorBase) getChildTask(0)).toString(sb, indention + 1);
		((QueryOperatorBase) getChildTask(1)).toString(sb, indention + 1);
	}

	private static enum JoinType {
		JOIN, CARTESIAN_PRODUCT, LEFT_FORWARD, RIGHT_FORWARD;
	}
}
