package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching;

/**
 * Generic doubly-linked node for a {@link DoublyLinkedList}, can contain parametrized-typed content.
 *
 * @author Philipp Töws
 *
 * @param <C>
 *            The type of the content that a node contains.
 */
public class DoublyLinkedNode<C> {
	DoublyLinkedNode<C> before, after;
	C content;

	@Override
	public String toString() {
		return getClass().getSimpleName() + '@' + Integer.toHexString(hashCode());
	}

	public static class KeyValueContent<K, V> {
		K key;
		V value;
	}

	public static class KeyValueSegmentContent<K, V, S> extends KeyValueContent<K, V> {
		S segment;
		int hits;
		// For investigations only
		long inCacheHits, inProtectedHits;
	}
}
