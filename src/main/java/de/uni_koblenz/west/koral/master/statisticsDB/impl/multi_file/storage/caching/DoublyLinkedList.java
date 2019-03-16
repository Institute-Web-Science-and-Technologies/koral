package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

/**
 * A general doubly-linked list, which nodes are key-value mappings.
 *
 * @author Philipp Töws
 *
 */
public class DoublyLinkedList<K, V> {
	private DoublyLinkedNode<K, V> head;

	private DoublyLinkedNode<K, V> tail;

	public DoublyLinkedNode<K, V> head() {
		return head;
	}

	public DoublyLinkedNode<K, V> tail() {
		return tail;
	}

	public void insertAfter(DoublyLinkedNode<K, V> predecessor, DoublyLinkedNode<K, V> node) {
		if (predecessor == tail) {
			append(node);
			return;
		}
		node.before = predecessor;
		if (predecessor != null) {
			node.after = predecessor.after;
			if (predecessor.after != null) {
				predecessor.after.before = node;
			}
			predecessor.after = node;
		} else {
			node.after = null;
		}
	}

	public void append(DoublyLinkedNode<K, V> node) {
		node.before = tail;
		node.after = null;
		if (tail != null) {
			tail.after = node;
		} else {
			assert head == null;
			head = node;
		}
		tail = node;
	}

	/**
	 * Removes a node from the DoublyLinkedList only.
	 *
	 * @param node
	 */
	protected void remove(DoublyLinkedNode<K, V> node) {
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
	 * Returns an iterator on the entries sorted by access order, starting from the last / the least recently used
	 * entry.
	 *
	 * @return
	 */
	public Iterator<Entry<K, V>> iteratorFromLast() {
		return new Iterator<Entry<K, V>>() {
			DoublyLinkedNode<K, V> nextNode = head;

			@Override
			public boolean hasNext() {
				return nextNode != null;
			}

			@Override
			public Entry<K, V> next() {
				if (!hasNext()) {
					throw new NoSuchElementException("Call hasNext() before next()");
				}
				DoublyLinkedNode<K, V> returnNode = nextNode;
				nextNode = nextNode.after;
				return new AbstractMap.SimpleImmutableEntry<>(returnNode.key, returnNode.value);
			}

		};
	}

	public boolean isEmpty() {
		return tail == null;
	}

	public void clear() {
		head = null;
		tail = null;
	}

}
