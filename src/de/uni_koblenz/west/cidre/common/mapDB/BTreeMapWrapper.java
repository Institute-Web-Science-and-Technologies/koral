package de.uni_koblenz.west.cidre.common.mapDB;

import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB.BTreeMapMaker;
import org.mapdb.Serializer;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * A wrapper for B-tree maps of MapDB.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 * @param <K>
 * @param <V>
 */
public class BTreeMapWrapper<K, V> extends MapDBMapWrapper<K, V> {

  private final BTreeMap<K, V> map;

  public BTreeMapWrapper(MapDBStorageOptions storageType, String databaseFile,
          boolean useTransactions, boolean writeAsynchronously, MapDBCacheOptions cacheType,
          String mapName, BTreeKeySerializer<K> keySerializer, Serializer<V> valueSerializer,
          boolean storeValuesOutsideOfNodes) {
    super(storageType, databaseFile, useTransactions, writeAsynchronously, cacheType);
    BTreeMapMaker treeMaker = database.createTreeMap(mapName).keySerializer(keySerializer)
            .valueSerializer(valueSerializer);
    if (storeValuesOutsideOfNodes) {
      treeMaker.valuesOutsideNodesEnable();
    }
    map = treeMaker.makeOrGet();
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public void close() {
    if (!database.isClosed()) {
      map.close();
    }
    super.close();
  }

  @Override
  public V putIfAbsent(K key, V value) {
    return map.putIfAbsent(key, value);
  }

  @Override
  public boolean remove(Object key, Object value) {
    return map.remove(key, value);
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    return map.replace(key, oldValue, newValue);
  }

  @Override
  public V replace(K key, V value) {
    return map.replace(key, value);
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return map.containsValue(value);
  }

  @Override
  public V get(Object key) {
    return map.get(key);
  }

  @Override
  public V put(K key, V value) {
    return map.put(key, value);
  }

  @Override
  public V remove(Object key) {
    return map.remove(key);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    map.putAll(m);
  }

  @Override
  public Set<K> keySet() {
    return map.keySet();
  }

  @Override
  public Collection<V> values() {
    return map.values();
  }

  @Override
  public Set<java.util.Map.Entry<K, V>> entrySet() {
    return map.entrySet();
  }

}
