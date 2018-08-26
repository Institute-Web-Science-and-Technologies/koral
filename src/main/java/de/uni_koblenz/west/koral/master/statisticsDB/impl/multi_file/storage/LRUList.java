package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A generic LRU list with O(1) operations and no capacity limits. Uses a doubly-linked-list for access order plus an
 * index (Map) for O(1) access.
 *
 * @author philipp
 *
 * @param <K>
 * @param <V>
 */
public class LRUList<K, V> implements Iterable<Entry<K, V>> {

	final Map<K, DoublyLinkedNode> index;
	// TODO: head should be the most recently used node
	DoublyLinkedNode head;

	DoublyLinkedNode tail;

	public LRUList() {
		index = new HashMap<>();
	}

	public void put(K key, V value) {
		DoublyLinkedNode node = new DoublyLinkedNode();
		node.key = key;
		node.value = value;
		access(node);
		if (head == null) {
			head = node;
		}
		DoublyLinkedNode oldValue = index.put(key, node);
		if (oldValue != null) {
			// Using put as update would result in memory leaks because the old value would
			// stay in the doubly-linked list
			throw new IllegalArgumentException(
					"Key " + key + " already exists. Use update() to change existing entries");
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

	void access(DoublyLinkedNode node) {
		if (node == tail) {
			return;
		}
		// Move to tail by removing from current position (if appearing in linked list)
		// and reinserting at the end
		if (head == node) {
			head = node.after;
		}
		if (node.after != null) {
			node.after.before = node.before;
		}
		if (node.before != null) {
			node.before.after = node.after;
		}
		// (Re)insert node
		node.before = tail;
		node.after = null;
		if (tail != null) {
			tail.after = node;
		}
		tail = node;
	}

	/**
	 * Removes a node from the DoublyLinkedList only.
	 *
	 * @param node
	 */
	void remove(DoublyLinkedNode node) {
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
	}

	/**
	 * Removes an entry by key from the DoublyLinkedList and the index.
	 *
	 * @param key
	 */
	public void remove(K key) {
		DoublyLinkedNode node = index.get(key);
		index.remove(key);
		remove(node);
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
		Iterator<Entry<K, LRUList<K, V>.DoublyLinkedNode>> iterator = index.entrySet().iterator();
		return new Iterator<Entry<K, V>>() {

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public Entry<K, V> next() {
				Entry<K, LRUList<K, V>.DoublyLinkedNode> next = iterator.next();
				Entry<K, V> result = new AbstractMap.SimpleImmutableEntry<>(next.getKey(), next.getValue().value);
				return result;
			}
		};
	}

	/**
	 * Returns an iterator on the entries sorted by access order, starting from the last / the least recently used
	 * entry.
	 *
	 * @return
	 */
	public Iterator<Entry<K, V>> iteratorFromLast() {
		return new Iterator<Entry<K, V>>() {
			DoublyLinkedNode nextNode = head;

			@Override
			public boolean hasNext() {
				return nextNode != null;
			}

			@Override
			public Entry<K, V> next() {
				if (!hasNext()) {
					throw new NoSuchElementException("Call hasNext() before next()");
				}
				DoublyLinkedNode returnNode = nextNode;
				nextNode = nextNode.after;
				return new AbstractMap.SimpleImmutableEntry<>(returnNode.key, returnNode.value);
			}

		};
	}

	void removeEldest() {
		DoublyLinkedNode eldest = head;
		remove(eldest);
		removeEldest(eldest.key, eldest.value);
	}

	/**
	 * @param value
	 *            Might be used in subimplementations
	 */
	protected void removeEldest(K key, V value) {
		index.remove(key);
	}

	public long size() {
		return index.size();
	}

	public boolean isEmpty() {
		return tail == null;
	}

	public void clear() {
		index.clear();
		head = null;
		tail = null;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("Cache List [");
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
