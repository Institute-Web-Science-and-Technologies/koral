package de.uni_koblenz.west.cidre.common.utils;

import java.util.Iterator;

import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;

public class MappingIteratorWrapper
		implements Iterable<Mapping>, Iterator<Mapping> {

	private final Iterator<byte[]> iter;

	private final MappingRecycleCache recycleCache;

	public MappingIteratorWrapper(Iterator<byte[]> iter,
			MappingRecycleCache recycleCache) {
		this.recycleCache = recycleCache;
		this.iter = iter;
	}

	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	@Override
	public Mapping next() {
		byte[] next = iter.next();
		return recycleCache.createMapping(next, 0, next.length);
	}

	@Override
	public Iterator<Mapping> iterator() {
		return this;
	}

}
