package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.counter;

public interface Counter<T> extends Iterable<T> {

	public void countFor(T element);

	public long getFrequency(T element);

	public void reset();

}
