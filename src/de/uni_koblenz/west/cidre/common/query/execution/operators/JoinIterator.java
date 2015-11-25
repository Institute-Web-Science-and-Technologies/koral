package de.uni_koblenz.west.cidre.common.query.execution.operators;

import java.util.Iterator;
import java.util.NoSuchElementException;

import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;

public class JoinIterator implements Iterator<Mapping> {

	private final MappingRecycleCache recycleCache;

	private final long[] joinVars;

	private final Mapping joiningMapping;

	private final Iterator<Mapping> joinCandidates;

	private Mapping next;

	public JoinIterator(MappingRecycleCache recycleCache, long[] joinVars,
			Mapping joiningMapping, Iterator<Mapping> joinCandidates) {
		super();
		this.recycleCache = recycleCache;
		this.joinVars = joinVars;
		this.joiningMapping = joiningMapping;
		this.joinCandidates = joinCandidates;
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
			if (areJoinVarValuesEqual(joiningMapping, joinCandidate)) {
				return recycleCache.mergeMappings(joiningMapping,
						joinCandidate);
			}
		}
		return null;
	}

	private boolean areJoinVarValuesEqual(Mapping mapping1, Mapping mapping2) {
		for (long var : joinVars) {
			if (mapping1.getValue(var) != mapping2.getValue(var)) {
				return false;
			}
		}
		return true;
	}

}
