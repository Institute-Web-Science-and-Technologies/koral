package de.uni_koblenz.west.cidre.slave.triple_store;

import java.io.Closeable;

public interface TripleStore extends Closeable, AutoCloseable {

	public void clear();

	@Override
	public void close();

}
