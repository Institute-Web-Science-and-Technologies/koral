package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

public class LRUCache<K, V> {

	private final int capacity;

	protected final Map<K, DoublyLinkedNode> index;

	protected DoublyLinkedNode head;

	protected DoublyLinkedNode tail;

	protected int size;

	public LRUCache(int capacity) {
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
		// Move to tail by removing from current position (if appearing in linked list) and reinserting at the end
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

	private void removeEldest() {
		DoublyLinkedNode eldest = head;
		remove(eldest);
		removeEldest(eldest);
	}

	protected void removeEldest(DoublyLinkedNode eldest) {
		index.remove(eldest.key);
	}

	public void clear() {
		index.clear();
		head = null;
		tail = null;
		size = 0;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("Cache(" + size + "): [");
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
