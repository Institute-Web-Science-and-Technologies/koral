package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

/**
 * A key-value storage that implements removing the "most useless" element according to a chosen strategy via
 * {@link #evict()}.
 *
 * @author Philipp Töws
 *
 * @param <K>
 *            Type of the keys
 * @param <V>
 *            Type of the values
 */
public interface Cache<K, V> extends Iterable<Entry<K, V>> {

	public void put(K key, V value);

	public void update(K key, V newValue);

	public V get(K key);

	public Set<K> keySet();

	public Collection<V> values();

	public Collection<Entry<K, V>> entrySet();

	public Iterator<Entry<K, V>> iteratorFromLast();

	public void remove(K key);

	public void evict();

	public void clear();

	public long size();

	public boolean isEmpty();

}
