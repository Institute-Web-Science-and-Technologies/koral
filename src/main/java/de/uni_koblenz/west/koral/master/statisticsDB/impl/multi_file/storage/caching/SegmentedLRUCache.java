package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.CentralLogger;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.SimpleConfiguration;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.SimpleConfiguration.ConfigurationKey;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching.DoublyLinkedNode.KeyValueSegmentContent;
import playground.StatisticsDBTest;

/**
 * LRU Cache with different segments (protected and probationary), with the idea, that often-accessed elements are
 * allowed to be stored in the protected segment, where evicts don't happen often.
 *
 * Implementation based on: R. Karedla, J. S. Love and B. G. Wherry, "Caching strategies to improve disk system
 * performance," in Computer, vol. 27, no. 3, pp. 38-46, March 1994. doi: 10.1109/2.268884 URL:
 * https://ieeexplore.ieee.org/document/268884
 *
 * @author Philipp Töws
 *
 * @param <K>
 *            Type of the key of the node content
 * @param <V>
 *            Type of the value of the node content
 */
public class SegmentedLRUCache<K, V> implements Cache<K, V> {

	private final int protectedMinHits;

	private final Map<K, DoublyLinkedNode<KeyValueSegmentContent<K, V, Segment>>> index;

	private final DoublyLinkedList<KeyValueSegmentContent<K, V, Segment>> list;

	/**
	 * The most recently used element of the probationary segment
	 */
	private DoublyLinkedNode<KeyValueSegmentContent<K, V, Segment>> mruProbationary;

	/**
	 * Current amount of elements in protected segment.
	 */
	private long protectedSize;

	private final long capacity;

	/**
	 * The maximum amount of elements in the protected segment
	 */
	private final long protectedLimit;

	private final DoublyLinkedNodeRecycler<KeyValueSegmentContent<K, V, Segment>> recycler;

	public static enum Segment {
		PROBATIONARY, PROTECTED
	}

	/**
	 *
	 * @param capacity
	 *            The maximum amount of elements the cache can hold in total
	 * @param protectedLimit
	 *            The maximum amount of elements in the protected segment
	 */
	public SegmentedLRUCache(long capacity, long protectedLimit,
			DoublyLinkedNodeRecycler<KeyValueSegmentContent<K, V, Segment>> recycler) {
		this.capacity = capacity;
		this.protectedLimit = protectedLimit;
		this.recycler = recycler;

		protectedMinHits = (int) SimpleConfiguration.getInstance().getValue(ConfigurationKey.SLRU_PROTECTED_MIN_HITS);

		index = new HashMap<>();
		list = new DoublyLinkedList<>();
	}

	@Override
	public void put(K key, V value) {
		DoublyLinkedNode<KeyValueSegmentContent<K, V, Segment>> node = recycler.retrieve();
		if (node == null) {
			node = new DoublyLinkedNode<>();
			node.content = new KeyValueSegmentContent<>();
		}
		node.content.key = key;
		node.content.value = value;
		node.content.segment = Segment.PROBATIONARY;
		node.content.hits = 1;
		node.content.inCacheHits = 1;

		if (mruProbationary != null) {
			list.insertBefore(mruProbationary, node);
		} else {
			list.append(node);
		}
		mruProbationary = node;

		if (list.size() > capacity) {
			evict();
		}

		if (StatisticsDBTest.LOG_SLRU_CACHE_SIZES) {
			CentralLogger.getInstance().addSizes(protectedSize, list.size());
		}

		DoublyLinkedNode<KeyValueSegmentContent<K, V, Segment>> oldValue = index.put(key, node);
		if (oldValue != null) {
			// Using put as update would result in memory leaks because the old value would
			// stay in the doubly-linked list
			throw new IllegalArgumentException(
					"Key " + key + " already exists. Use update() to change existing entries");
		}
	}

	@Override
	public void update(K key, V newValue) {
		DoublyLinkedNode<KeyValueSegmentContent<K, V, Segment>> node = index.get(key);
		if (node == null) {
			put(key, newValue);
			return;
		}
		access(node);
		node.content.value = newValue;
	}

	void access(DoublyLinkedNode<KeyValueSegmentContent<K, V, Segment>> node) {
		if (node.content.segment == Segment.PROBATIONARY) {
			node.content.hits++;
			if (node.content.hits < protectedMinHits) {
				if (node != mruProbationary) {
					list.remove(node);
					list.insertBefore(mruProbationary, node);
					mruProbationary = node;
				}
			} else {
				if (node == mruProbationary) {
					mruProbationary = mruProbationary.after;
				}
				list.remove(node);
				list.prepend(node);
				protectedSize++;
				node.content.segment = Segment.PROTECTED;
				if (protectedSize > protectedLimit) {
					evictProtected();
				}
			}
		} else {
			if (node != list.head()) {
				list.remove(node);
				list.prepend(node);
			}
		}
	}

	@Override
	public V get(K key) {
		DoublyLinkedNode<KeyValueSegmentContent<K, V, Segment>> node = index.get(key);
		if (node == null) {
			return null;
		}
		access(node);
		if (node.content.segment == Segment.PROBATIONARY) {
			node.content.inCacheHits++;
		} else if (node.content.segment == Segment.PROTECTED) {
			node.content.inCacheHits++;
			node.content.inProtectedHits++;
		}
		return node.content.value;
	}

	@Override
	public Iterator<Entry<K, V>> iterator() {
		Iterator<Entry<K, DoublyLinkedNode<KeyValueSegmentContent<K, V, Segment>>>> iterator = index.entrySet()
				.iterator();
		return new Iterator<Entry<K, V>>() {

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public Entry<K, V> next() {
				Entry<K, DoublyLinkedNode<KeyValueSegmentContent<K, V, Segment>>> next = iterator.next();
				Entry<K, V> result = new AbstractMap.SimpleImmutableEntry<>(next.getKey(),
						next.getValue().content.value);
				return result;
			}
		};
	}

	@Override
	public void remove(K key) {
		DoublyLinkedNode<KeyValueSegmentContent<K, V, Segment>> node = index.get(key);
		if (node.content.segment == Segment.PROTECTED) {
			protectedSize--;
		}
		if (node == mruProbationary) {
			mruProbationary = mruProbationary.after;
		}
		list.remove(node);
		index.remove(key);
	}

	@Override
	public void evict() {
		DoublyLinkedNode<KeyValueSegmentContent<K, V, Segment>> lru = list.tail();
		// Cannot be protected (as long as protected max capacity is less than total capacity and evict is only called
		// on full cache)
		assert lru.content.segment == Segment.PROBATIONARY;
		if (mruProbationary == lru) {
			mruProbationary = mruProbationary.before;
			if (mruProbationary.content.segment == Segment.PROTECTED) {
				// We went to far, no probationary elements left
				mruProbationary = null;
			}
		}
		list.remove(lru);
		if (StatisticsDBTest.LOG_SLRU_CACHE_HITS) {
			CentralLogger.getInstance().addInCacheHits(lru.content.inCacheHits);
		}
		recycler.dump(lru);
		removeEldest(lru.content.key, lru.content.value);
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

	/**
	 * Evicts protected segment by moving the LRU element to the MRU end of the probationary segment by shifting the
	 * {@link #mruProbationary} pointer to the previous element.
	 */
	private void evictProtected() {
		assert (protectedSize - 1) == protectedLimit;
		DoublyLinkedNode<KeyValueSegmentContent<K, V, Segment>> lruProtected;
		if (mruProbationary != null) {
			lruProtected = mruProbationary.before;
		} else {
			lruProtected = list.tail();
		}
		mruProbationary = lruProtected;
		mruProbationary.content.segment = Segment.PROBATIONARY;
		mruProbationary.content.hits = 0;
		protectedSize--;
		if (StatisticsDBTest.LOG_SLRU_CACHE_HITS) {
			CentralLogger.getInstance().addInProtectedHits(lruProtected.content.inProtectedHits);
		}
		lruProtected.content.inProtectedHits = 0;
	}

	@Override
	public void clear() {
		list.clear();
		index.clear();
		protectedSize = 0;
		mruProbationary = null;
	}

	@Override
	public long size() {
		return list.size();
	}

	@Override
	public boolean isEmpty() {
		return list.isEmpty();
	}

	@Override
	public Set<K> keySet() {
		return index.keySet();
	}

	@Override
	public Collection<V> values() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<Entry<K, V>> entrySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<Entry<K, V>> iteratorFromLast() {
		throw new UnsupportedOperationException();
	}

}
