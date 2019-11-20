package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A general doubly-linked list, whose nodes contain container classes as content.
 *
 * @author Philipp Töws
 *
 */
public class DoublyLinkedList<C> {
	private DoublyLinkedNode<C> head;

	private DoublyLinkedNode<C> tail;

	private long size;

	public DoublyLinkedNode<C> head() {
		return head;
	}

	public DoublyLinkedNode<C> tail() {
		return tail;
	}

	public void insertAfter(DoublyLinkedNode<C> predecessor, DoublyLinkedNode<C> node) {
		if (predecessor == null) {
			throw new NullPointerException();
		}
		if (predecessor == tail) {
			append(node);
			return;
		}
		node.before = predecessor;
		node.after = predecessor.after;
		if (predecessor.after != null) {
			predecessor.after.before = node;
		}
		predecessor.after = node;
		size++;
	}

	public void insertBefore(DoublyLinkedNode<C> successor, DoublyLinkedNode<C> node) {
		if (successor == null) {
			throw new NullPointerException();
		}
		if (successor == head) {
			prepend(node);
			return;
		}
		node.after = successor;
		node.before = successor.before;
		if (successor.before != null) {
			successor.before.after = node;
		}
		successor.before = node;
		size++;
	}

	public void prepend(DoublyLinkedNode<C> node) {
		node.before = null;
		node.after = head;
		if (head != null) {
			head.before = node;
		} else {
			assert tail == null;
			tail = node;
		}
		head = node;
		size++;
	}

	public void append(DoublyLinkedNode<C> node) {
		node.before = tail;
		node.after = null;
		if (tail != null) {
			tail.after = node;
		} else {
			assert head == null;
			head = node;
		}
		tail = node;
		size++;
	}

	protected void remove(DoublyLinkedNode<C> node) {
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

	/**
	 * Returns an iterator on the entries sorted by access order, starting from the last / the least recently used
	 * entry.
	 *
	 * @return
	 */
	public Iterator<C> iteratorFromLast() {
		return new Iterator<C>() {
			DoublyLinkedNode<C> nextNode = head;

			@Override
			public boolean hasNext() {
				return nextNode != null;
			}

			@Override
			public C next() {
				if (!hasNext()) {
					throw new NoSuchElementException("Call hasNext() before next()");
				}
				DoublyLinkedNode<C> returnNode = nextNode;
				nextNode = nextNode.after;
				return returnNode.content;
			}

		};
	}

	public boolean isEmpty() {
		return tail == null;
	}

	public long size() {
		return size;
	}

	public void clear() {
		head = null;
		tail = null;
		size = 0;
	}

}
