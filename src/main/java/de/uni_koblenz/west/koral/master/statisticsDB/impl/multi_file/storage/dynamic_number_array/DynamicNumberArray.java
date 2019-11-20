package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.dynamic_number_array;

/**
 * General array that can dynamically vary it's size to store and access numerical values. Both per-value bit capacity
 * and overall capacity of numbers can increase. Attempt to replace RLE array with fixed size.
 *
 * @author Philipp Töws
 *
 */

public interface DynamicNumberArray {

	public int capacity();

	public long get(int index);

	public void set(int index, long value, Caller caller);

	public void inc(int index, Caller caller);

	public void dec(int index, Caller caller);

	public void copy(int srcIndex, int destIndex, int length, Caller caller);

	public void move(int srcIndex, int destIndex, Caller caller);

	public void insertGap(int index, int length, Caller caller);

	public int getLastUsedIndex();
}
