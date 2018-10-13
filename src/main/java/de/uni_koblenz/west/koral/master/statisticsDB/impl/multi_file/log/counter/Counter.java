package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.counter;

public interface Counter<T> extends AutoCloseable, Iterable<T> {

	public void countFor(T element);

	public long getFrequency(T element);

	public void reset();

	/**
	 * Is supposed to delete any created data and release all resources.
	 */
	@Override
	public void close();
}
