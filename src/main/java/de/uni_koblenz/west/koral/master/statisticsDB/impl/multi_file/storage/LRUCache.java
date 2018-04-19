package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * A generic LRU cache with O(1) operations. Uses a doubly-linked-list for access order plus an index (Map) for O(1)
 * access.
 *
 * @author philipp
 *
 * @param <K>
 * @param <V>
 */
public class LRUCache<K, V> implements Iterable<Entry<K, V>> {

	private final long capacity;

	final Map<K, DoublyLinkedNode> index;

	DoublyLinkedNode head;

	DoublyLinkedNode tail;

	long size;

	public LRUCache(long capacity) {
		this.capacity = capacity;

		index = new HashMap<>();
	}

	public V put(K key, V value) {
		DoublyLinkedNode node = new DoublyLinkedNode();
		node.key = key;
		node.value = value;
		access(node);
		if (head == null) {
			head = node;
		}
		DoublyLinkedNode oldValue = index.put(key, node);
		if (oldValue != null) {
			return oldValue.value;
		} else {
			return null;
		}
	}

	/**
	 * Creates entry if it doesn't exist.
	 *
	 * @param key
	 * @param newValue
	 */
	public void update(K key, V newValue) {
		DoublyLinkedNode node = index.get(key);
		if (node == null) {
			put(key, newValue);
			return;
		}
		access(node);
		node.value = newValue;
	}

	public V get(K key) {
		DoublyLinkedNode node = index.get(key);
		if (node == null) {
			return null;
		}
		access(node);
		return node.value;
	}

	private void access(DoublyLinkedNode node) {
		if (node == tail) {
			return;
		}
		// Move to tail by removing from current position (if appearing in linked list)
		// and reinserting at the end
		if ((node.before != null) || (node.after != null)) {
			remove(node);
		}
		if (size >= capacity) {
			removeEldest();
		}

		node.before = tail;
		node.after = null;
		if (tail != null) {
			tail.after = node;
		}
		tail = node;
		size++;
	}

	private void remove(DoublyLinkedNode node) {
		if (head == node) {
			head = node.after;
		}
		if (tail == node) {
			tail = node.before;
		}
		if (node.after != null) {
			node.after.before = node.before;
		}
		if (node.before != null) {
			node.before.after = node.after;
		}
		node.before = null;
		node.after = null;
		size--;
	}

	public Set<K> keySet() {
		Set<K> keys = new HashSet<>();
		for (K key : index.keySet()) {
			keys.add(key);
		}
		return keys;
	}

	public Collection<V> values() {
		LinkedList<V> values = new LinkedList<>();
		for (DoublyLinkedNode node : index.values()) {
			values.add(node.value);
		}
		return values;
	}

	public Collection<Entry<K, V>> entrySet() {
		LinkedList<Entry<K, V>> entrySet = new LinkedList<>();
		for (Entry<K, DoublyLinkedNode> entry : index.entrySet()) {
			entrySet.add(new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), entry.getValue().value));
		}
		return entrySet;
	}

	@Override
	public Iterator<Entry<K, V>> iterator() {
		Iterator<Entry<K, LRUCache<K, V>.DoublyLinkedNode>> iterator = index.entrySet().iterator();
		return new Iterator<Entry<K, V>>() {

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public Entry<K, V> next() {
				Entry<K, LRUCache<K, V>.DoublyLinkedNode> next = iterator.next();
				Entry<K, V> result = new AbstractMap.SimpleImmutableEntry<>(next.getKey(), next.getValue().value);
				return result;
			}
		};
	}

	private void removeEldest() {
		DoublyLinkedNode eldest = head;
		remove(eldest);
		removeEldest(eldest.key, eldest.value);
	}

	protected void removeEldest(K key, V value) {
		index.remove(key);
	}

	/**
	 * @return The amount of elements in the cache, that is the doubly linked list. There may be more in the index map,
	 *         depending on sub-implementations of {@link #removeEldest(DoublyLinkedNode)}.
	 */
	public long size() {
		return size;
	}

	public void clear() {
		index.clear();
		head = null;
		tail = null;
		size = 0;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("Cache size: " + size + " [");
		for (DoublyLinkedNode node = head; node != null; node = node.after) {
			sb.append(node.key).append("=").append(node.value);
			if (node.after != null) {
				sb.append(", ");
			}
		}
		sb.append("]");
		sb.append("\nIndex: ");
		sb.append(index.toString());
		return sb.toString();
	}

	class DoublyLinkedNode {
		DoublyLinkedNode before, after;
		K key;
		V value;

		@Override
		public String toString() {
			return getClass().getSimpleName() + '@' + Integer.toHexString(hashCode());
		}
	}

}
