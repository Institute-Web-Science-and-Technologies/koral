package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.counter;

import java.io.Closeable;
import java.util.Iterator;

public interface PersistantStore extends Closeable, Iterable<byte[]> {

	/**
	 * Returns value of provided key.
	 *
	 * @param key
	 * @return Associated value, null if non-existent.
	 */
	public byte[] get(byte[] key);

	public void put(byte[] key, byte[] value);

	@Override
	public Iterator<byte[]> iterator();

	public void flush();

	public void reset();

	@Override
	public void close();
}
