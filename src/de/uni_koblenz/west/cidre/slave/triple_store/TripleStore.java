package de.uni_koblenz.west.cidre.slave.triple_store;

import java.io.Closeable;

import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;
import de.uni_koblenz.west.cidre.common.query.TriplePattern;

public interface TripleStore extends Closeable, AutoCloseable {

	public void storeTriple(long subject, long property, long object,
			byte[] containment);

	public Iterable<Mapping> lookup(MappingRecycleCache cache,
			TriplePattern triplePattern);

	public void clear();

	@Override
	public void close();

}
