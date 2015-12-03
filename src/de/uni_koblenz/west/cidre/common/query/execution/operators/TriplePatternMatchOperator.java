package de.uni_koblenz.west.cidre.common.query.execution.operators;

import java.io.File;
import java.util.Iterator;

import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.TriplePattern;
import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorBase;
import de.uni_koblenz.west.cidre.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.cidre.slave.triple_store.TripleStoreAccessor;

/**
 * Performs the match of a triple pattern.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class TriplePatternMatchOperator extends QueryOperatorBase {

	private final TriplePattern pattern;

	private final TripleStoreAccessor tripleStore;

	private Iterator<Mapping> iterator;

	public TriplePatternMatchOperator(long id, long coordinatorId,
			int numberOfSlaves, int cacheSize, File cacheDirectory,
			TriplePattern pattern, int emittedMappingsPerRound,
			TripleStoreAccessor tripleStore) {
		super(id, coordinatorId, numberOfSlaves, cacheSize, cacheDirectory,
				emittedMappingsPerRound);
		this.pattern = pattern;
		this.tripleStore = tripleStore;
	}

	public TriplePatternMatchOperator(short slaveId, int queryId, short taskId,
			long coordinatorId, int numberOfSlaves, int cacheSize,
			File cacheDirectory, TriplePattern pattern,
			int emittedMappingsPerRound, TripleStoreAccessor tripleStore) {
		super(slaveId, queryId, taskId, coordinatorId, numberOfSlaves,
				cacheSize, cacheDirectory, emittedMappingsPerRound);
		this.pattern = pattern;
		this.tripleStore = tripleStore;
	}

	@Override
	public long computeEstimatedLoad(GraphStatistics statistics, int slave) {
		switch (pattern.getType()) {
		case ___:
			return statistics.getChunkSizes()[slave];
		case S__:
			return slave < 0
					? statistics.getTotalSubjectFrequency(pattern.getSubject())
					: statistics.getSubjectFrequency(pattern.getSubject(),
							slave);
		case _P_:
			return slave < 0
					? statistics
							.getTotalPropertyFrequency(pattern.getProperty())
					: statistics.getPropertyFrequency(pattern.getProperty(),
							slave);
		case __O:
			return slave < 0
					? statistics.getTotalObjectFrequency(pattern.getObject())
					: statistics.getObjectFrequency(pattern.getObject(), slave);
		case SP_:
			long subjectFrequency = slave < 0
					? statistics.getTotalSubjectFrequency(pattern.getSubject())
					: statistics.getSubjectFrequency(pattern.getSubject(),
							slave);
			if (subjectFrequency == 0) {
				return 0;
			}
			long propertyFrequency = slave < 0
					? statistics
							.getTotalPropertyFrequency(pattern.getProperty())
					: statistics.getPropertyFrequency(pattern.getProperty(),
							slave);
			if (subjectFrequency < propertyFrequency) {
				return subjectFrequency;
			} else {
				return propertyFrequency;
			}
		case S_O:
			subjectFrequency = slave < 0
					? statistics.getTotalSubjectFrequency(pattern.getSubject())
					: statistics.getSubjectFrequency(pattern.getSubject(),
							slave);
			if (subjectFrequency == 0) {
				return 0;
			}
			long objectFrequency = slave < 0
					? statistics.getTotalObjectFrequency(pattern.getObject())
					: statistics.getObjectFrequency(pattern.getObject(), slave);
			if (subjectFrequency < objectFrequency) {
				return subjectFrequency;
			} else {
				return objectFrequency;
			}
		case _PO:
			propertyFrequency = slave < 0
					? statistics
							.getTotalPropertyFrequency(pattern.getProperty())
					: statistics.getPropertyFrequency(pattern.getProperty(),
							slave);
			if (propertyFrequency == 0) {
				return 0;
			}
			objectFrequency = slave < 0
					? statistics.getTotalObjectFrequency(pattern.getObject())
					: statistics.getObjectFrequency(pattern.getObject(), slave);
			if (propertyFrequency < objectFrequency) {
				return propertyFrequency;
			} else {
				return objectFrequency;
			}
		case SPO:
			subjectFrequency = slave < 0
					? statistics.getTotalSubjectFrequency(pattern.getSubject())
					: statistics.getSubjectFrequency(pattern.getSubject(),
							slave);
			if (subjectFrequency == 0) {
				return 0;
			}
			propertyFrequency = slave < 0
					? statistics
							.getTotalPropertyFrequency(pattern.getProperty())
					: statistics.getPropertyFrequency(pattern.getProperty(),
							slave);
			if (propertyFrequency == 0) {
				return 0;
			}
			objectFrequency = slave < 0
					? statistics.getTotalObjectFrequency(pattern.getObject())
					: statistics.getObjectFrequency(pattern.getObject(), slave);
			if (objectFrequency == 0) {
				return 0;
			} else {
				return 1;
			}
		}
		return 0;
	}

	@Override
	public long computeTotalEstimatedLoad(GraphStatistics statistics) {
		return computeEstimatedLoad(statistics, -1);
	}

	@Override
	public long[] getResultVariables() {
		return pattern.getVariables();
	}

	@Override
	public long getFirstJoinVar() {
		long[] vars = pattern.getVariables();
		long min = Long.MAX_VALUE;
		for (long var : vars) {
			if (var < min) {
				min = var;
			}
		}
		return min;
	}

	@Override
	public long getCurrentTaskLoad() {
		if (iterator == null || tripleStore == null
				|| getEstimatedTaskLoad() == 0 || !iterator.hasNext()) {
			return 0;
		} else {
			return getEmittedMappingsPerRound();
		}
	}

	@Override
	protected void closeInternal() {
	}

	@Override
	protected void executeOperationStep() {
		if (getEstimatedTaskLoad() == 0 || tripleStore == null) {
			return;
		}
		if (iterator == null) {
			iterator = tripleStore.lookup(recycleCache, pattern).iterator();
		}
		for (int i = 0; i < getEmittedMappingsPerRound()
				&& iterator.hasNext(); i++) {
			emitMapping(iterator.next());
		}
	}

	@Override
	protected void executeFinalStep() {
	}

	@Override
	protected boolean isFinishedInternal() {
		return getEstimatedTaskLoad() == 0 || tripleStore == null
				|| (iterator != null && !iterator.hasNext());
	}

	@Override
	public void toString(StringBuilder sb, int indention) {
		indent(sb, indention);
		sb.append(getClass().getSimpleName());
		sb.append(" ").append(pattern.isSubjectVariable() ? "?" : "")
				.append(pattern.getSubject());
		sb.append(" ").append(pattern.isPropertyVariable() ? "?" : "")
				.append(pattern.getProperty());
		sb.append(" ").append(pattern.isObjectVariable() ? "?" : "")
				.append(pattern.getObject());
		sb.append("\n");
	}

}
