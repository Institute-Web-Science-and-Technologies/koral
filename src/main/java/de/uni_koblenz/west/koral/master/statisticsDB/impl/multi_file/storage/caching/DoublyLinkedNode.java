package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching;

public class DoublyLinkedNode<K, V> {
	DoublyLinkedNode<K, V> before, after;
	K key;
	V value;

	@Override
	public String toString() {
		return getClass().getSimpleName() + '@' + Integer.toHexString(hashCode());
	}
}
