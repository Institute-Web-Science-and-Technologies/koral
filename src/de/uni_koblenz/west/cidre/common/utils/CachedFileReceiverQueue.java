package de.uni_koblenz.west.cidre.common.utils;

import java.io.Closeable;
import java.io.File;

import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;

/**
 * Caches received mappings until a limit is reached. Thereafter, mappings are
 * written to files.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class CachedFileReceiverQueue implements Closeable {

	// TODO queues have to be synchronized

	public CachedFileReceiverQueue(int cacheSize, File directory) {
		// TODO Auto-generated constructor stub
	}

	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}

	public long size() {
		// TODO Auto-generated method stub
		return 0;
	}

	public void enqueue(byte[] message, int firstIndex) {
		// TODO Auto-generated method stub

	}

	public Mapping dequeue(MappingRecycleCache recycleCache) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
