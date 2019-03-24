package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching.DoublyLinkedNode.KeyValueContent;

/**
 * A generic LRU list with O(1) operations and no capacity limits. Uses a doubly-linked-list for access order plus an
 * index (Map) for O(1) access.
 *
 * @author philipp
 *
 * @param <K>
 * @param <V>
 */
public class LRUList<K, V> implements Cache<K, V> {

	private final Map<K, DoublyLinkedNode<KeyValueContent<K, V>>> index;

	protected final DoublyLinkedList<KeyValueContent<K, V>> list;

	public LRUList() {
		index = new HashMap<>();
		list = new DoublyLinkedList<>();
	}

	@Override
	public void put(K key, V value) {
		DoublyLinkedNode<KeyValueContent<K, V>> node = new DoublyLinkedNode<>();
		node.content = new KeyValueContent<>();
		node.content.key = key;
		node.content.value = value;
		list.append(node);
		DoublyLinkedNode<KeyValueContent<K, V>> oldValue = index.put(key, node);
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
	@Override
	public void update(K key, V newValue) {
		DoublyLinkedNode<KeyValueContent<K, V>> node = index.get(key);
		if (node == null) {
			put(key, newValue);
			return;
		}
		access(node);
		node.content.value = newValue;
	}

	@Override
	public V get(K key) {
		DoublyLinkedNode<KeyValueContent<K, V>> node = index.get(key);
		if (node == null) {
			return null;
		}
		access(node);
		return node.content.value;
	}

	void access(DoublyLinkedNode<KeyValueContent<K, V>> node) {
		if (node == list.tail()) {
			return;
		}
		list.remove(node);
		list.append(node);
	}

	/**
	 * Removes an entry by key from both the DoublyLinkedList and the index.
	 *
	 * @param key
	 */
	@Override
	public void remove(K key) {
		DoublyLinkedNode<KeyValueContent<K, V>> node = index.get(key);
		index.remove(key);
		list.remove(node);
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
		for (DoublyLinkedNode<KeyValueContent<K, V>> node : index.values()) {
			values.add(node.content.value);
		}
		return values;
	}

	public Collection<Entry<K, V>> entrySet() {
		LinkedList<Entry<K, V>> entrySet = new LinkedList<>();
		for (Entry<K, DoublyLinkedNode<KeyValueContent<K, V>>> entry : index.entrySet()) {
			entrySet.add(new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), entry.getValue().content.value));
		}
		return entrySet;
	}

	@Override
	public Iterator<Entry<K, V>> iterator() {
		Iterator<Entry<K, DoublyLinkedNode<KeyValueContent<K, V>>>> iterator = index.entrySet().iterator();
		return new Iterator<Entry<K, V>>() {

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public Entry<K, V> next() {
				Entry<K, DoublyLinkedNode<KeyValueContent<K, V>>> next = iterator.next();
				Entry<K, V> result = new AbstractMap.SimpleImmutableEntry<>(next.getKey(),
						next.getValue().content.value);
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
		Iterator<KeyValueContent<K, V>> iterator = list.iteratorFromLast();
		return new Iterator<Entry<K, V>>() {

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public Entry<K, V> next() {
				KeyValueContent<K, V> nextContent = iterator.next();
				return new AbstractMap.SimpleImmutableEntry<>(nextContent.key, nextContent.value);
			}

		};
	}

	/**
	 * Removes the oldest element from the list, and if not overwritten by subimplementations also from the index.
	 */
	@Override
	public void evict() {
		DoublyLinkedNode<KeyValueContent<K, V>> eldest = list.head();
		list.remove(eldest);
		removeEldest(eldest.content.key, eldest.content.value);
	}

	/**
	 * Removes the oldest element which is given via the parameters from the index. Subimplementations might disable
	 * this deletion or do something with the removed element.
	 *
	 * @param value
	 *            Might be used in subimplementations
	 */
	protected void removeEldest(K key, V value) {
		index.remove(key);
	}

	@Override
	public long size() {
		return index.size();
	}

	@Override
	public boolean isEmpty() {
		return list.isEmpty();
	}

	@Override
	public void clear() {
		index.clear();
		list.clear();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("LRU List [");
		for (DoublyLinkedNode<KeyValueContent<K, V>> node = list.head(); node != null; node = node.after) {
			sb.append(node.content.key).append("=").append(node.content.value);
			if (node.after != null) {
				sb.append(", ");
			}
		}
		sb.append("]");
		sb.append("\nIndex: ");
		sb.append(index.toString());
		return sb.toString();
	}

}
