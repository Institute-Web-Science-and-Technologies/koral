package de.uni_koblenz.west.koral.master.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

/**
 * An in memory {@link SimpleLongMap}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class InMemorySimpleLongMap implements SimpleLongMap {

  private final Map<Long, Long> map;

  public InMemorySimpleLongMap() {
    map = new HashMap<>();
  }

  @Override
  public void put(long key, long value) {
    map.put(key, value);
  }

  @Override
  public long get(long key) {
    Long value = map.get(key);
    if (value == null) {
      throw new NoSuchElementException();
    } else {
      return value;
    }
  }

  @Override
  public Iterator<long[]> iterator() {
    return new Iterator<long[]>() {

      private final Iterator<Entry<Long, Long>> iterator = map.entrySet().iterator();

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public long[] next() {
        Entry<Long, Long> entry = iterator.next();
        return new long[] { entry.getKey(), entry.getValue() };
      }
    };
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() {
    map.clear();
  }

}
