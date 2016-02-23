package de.uni_koblenz.west.cidre.common.query.execution.operators;

import java.util.Iterator;
import java.util.NoSuchElementException;

import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;

public class JoinIterator implements Iterator<Mapping> {

	private final MappingRecycleCache recycleCache;

	private final long[] resultVars;

	private final long[] joinVars;

	private final Mapping joiningMapping;

	private final long[] varsOfJoiningMapping;

	private final Iterator<Mapping> joinCandidates;

	private final long[] varsOfJoinCandidates;

	private Mapping next;

	public JoinIterator(MappingRecycleCache recycleCache, long[] resultVars,
			long[] joinVars, Mapping joiningMapping,
			long[] varsOfJoiningMapping, Iterator<Mapping> joinCandidates,
			long[] varsOfJoinCandidates) {
		super();
		this.recycleCache = recycleCache;
		this.resultVars = resultVars;
		this.joinVars = joinVars;
		this.joiningMapping = joiningMapping;
		this.varsOfJoiningMapping = varsOfJoiningMapping;
		this.joinCandidates = joinCandidates;
		this.varsOfJoinCandidates = varsOfJoinCandidates;
		next = getNext();
	}

	@Override
	public boolean hasNext() {
		return next != null;
	}

	@Override
	public Mapping next() {
		if (next == null) {
			throw new NoSuchElementException();
		}
		Mapping n = next;
		next = getNext();
		return n;
	}

	private Mapping getNext() {
		while (joinCandidates.hasNext()) {
			Mapping joinCandidate = joinCandidates.next();
			if (areJoinVarValuesEqual(joiningMapping, varsOfJoiningMapping,
					joinCandidate, varsOfJoinCandidates)) {
				return recycleCache.mergeMappings(resultVars, joiningMapping,
						varsOfJoiningMapping, joinCandidate,
						varsOfJoinCandidates);
			}
		}
		return null;
	}

	private boolean areJoinVarValuesEqual(Mapping mapping1, long[] vars1,
			Mapping mapping2, long[] vars2) {
		for (long var : joinVars) {
			if (mapping1.getValue(var, vars1) != mapping2.getValue(var,
					vars2)) {
				return false;
			}
		}
		return true;
	}

}
