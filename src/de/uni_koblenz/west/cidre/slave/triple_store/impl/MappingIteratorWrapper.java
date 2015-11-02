package de.uni_koblenz.west.cidre.slave.triple_store.impl;

import java.util.Iterator;

import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;
import de.uni_koblenz.west.cidre.common.query.TriplePattern;

public class MappingIteratorWrapper
		implements Iterable<Mapping>, Iterator<Mapping> {

	private final MappingRecycleCache cache;

	private final IndexType indexType;

	private final TriplePattern pattern;

	private final Iterator<byte[]> iter;

	public MappingIteratorWrapper(MappingRecycleCache cache,
			IndexType indexType, TriplePattern pattern, Iterator<byte[]> iter) {
		this.cache = cache;
		this.indexType = indexType;
		this.pattern = pattern;
		this.iter = iter;
	}

	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	@Override
	public Mapping next() {
		byte[] triple = iter.next();
		return cache.createMapping(indexType.getSubject(triple),
				indexType.getProperty(triple), indexType.getObject(triple),
				pattern, indexType.getContainment(triple));
	}

	@Override
	public Iterator<Mapping> iterator() {
		return this;
	}

}
