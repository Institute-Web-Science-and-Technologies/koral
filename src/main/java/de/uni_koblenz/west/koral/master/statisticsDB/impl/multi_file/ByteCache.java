package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

interface ByteCache {

	/**
	 *
	 * @param offset
	 * @param data
	 * @return True if there was enough space in the cache, false otherwise.
	 */
	boolean writeCache(int offset, byte[] data);

	byte[] readCache(int offset, int length);

	byte[] getData();
}
