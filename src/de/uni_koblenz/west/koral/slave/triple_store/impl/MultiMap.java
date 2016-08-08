package de.uni_koblenz.west.koral.slave.triple_store.impl;

import java.io.Closeable;
import java.util.Iterator;

/**
 * A multi map.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface MultiMap extends Closeable, AutoCloseable, Iterable<byte[]> {

  public boolean containsKey(byte[] prefix);

  public Iterable<byte[]> get(byte[] prefix);

  public void put(byte[] content);

  public void removeAll(byte[] prefix);

  public void remove(byte[] content);

  @Override
  public Iterator<byte[]> iterator();

  public void clear();

  @Override
  public void close();

}
