package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching.DoublyLinkedNode.KeyValueSegmentContent;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching.SegmentedLRUCache.Segment;

/**
 * Stores {@link DoublyLinkedNode}s to allow recycling without having to create new objects.
 * 
 * @author Philipp Töws
 *
 * @param <C>
 */
public class DoublyLinkedNodeRecycler<C> {

	private final DoublyLinkedList<C> nodes;

	private final int capacity;

	long retrieved, maxUsage;

	private static DoublyLinkedNodeRecycler<KeyValueSegmentContent<Long, byte[], Segment>> segmentedLRUCacheRecycler;

	public static DoublyLinkedNodeRecycler<KeyValueSegmentContent<Long, byte[], Segment>> getSegmentedLRUCacheRecycler() {
		if (segmentedLRUCacheRecycler == null) {
			segmentedLRUCacheRecycler = new DoublyLinkedNodeRecycler<>(1000);
		}
		return segmentedLRUCacheRecycler;
	}

	public DoublyLinkedNodeRecycler(int capacity) {
		this.capacity = capacity;
		nodes = new DoublyLinkedList<>();
	}

	public void dump(DoublyLinkedNode<C> node) {
		if (nodes.size() < capacity) {
			nodes.append(node);
			if (nodes.size() > maxUsage) {
				maxUsage = nodes.size();
			}
		}
	}

	public DoublyLinkedNode<C> retrieve() {
		if (nodes.size() > 0) {
			retrieved++;
			DoublyLinkedNode<C> head = nodes.head();
			nodes.remove(head);
			return head;
		} else {
			return null;
		}
	}

	public void printStats() {
		if (retrieved > 0) {
			System.out.println("DLN-Recycler recycled " + retrieved + " and had a max usage of " + maxUsage);
			retrieved = 0;
			maxUsage = 0;
		}
	}
}
