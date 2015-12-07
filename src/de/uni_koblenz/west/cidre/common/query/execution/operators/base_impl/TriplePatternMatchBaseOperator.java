package de.uni_koblenz.west.cidre.common.query.execution.operators.base_impl;

import java.io.File;

import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.TriplePattern;
import de.uni_koblenz.west.cidre.common.query.execution.operators.TriplePatternMatchOperator;
import de.uni_koblenz.west.cidre.slave.triple_store.TripleStoreAccessor;

public class TriplePatternMatchBaseOperator extends TriplePatternMatchOperator {

	private static final long serialVersionUID = 1162712420312094580L;

	public TriplePatternMatchBaseOperator(long id, long coordinatorId,
			int numberOfSlaves, int cacheSize, File cacheDirectory,
			TriplePattern pattern, int emittedMappingsPerRound,
			TripleStoreAccessor tripleStore) {
		super(id, coordinatorId, numberOfSlaves, cacheSize, cacheDirectory,
				pattern, emittedMappingsPerRound, tripleStore);
	}

	public TriplePatternMatchBaseOperator(short slaveId, int queryId,
			short taskId, long coordinatorId, int numberOfSlaves, int cacheSize,
			File cacheDirectory, TriplePattern pattern,
			int emittedMappingsPerRound, TripleStoreAccessor tripleStore) {
		super(slaveId, queryId, taskId, coordinatorId, numberOfSlaves,
				cacheSize, cacheDirectory, pattern, emittedMappingsPerRound,
				tripleStore);
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
