package de.uni_koblenz.west.koral.master.utils;

import java.io.Closeable;
import java.util.NoSuchElementException;

/**
 * A simple map consisting of long values.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface SimpleLongMap extends Closeable, Iterable<long[]> {

  /**
   * @param key
   * @param value
   */
  public void put(long key, long value);

  /**
   * @param key
   * @return
   */
  public long get(long key) throws NoSuchElementException;

  public void flush();

  @Override
  public void close();

}
