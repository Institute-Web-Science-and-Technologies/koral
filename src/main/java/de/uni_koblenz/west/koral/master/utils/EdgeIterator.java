package de.uni_koblenz.west.koral.master.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * 
 * Iterates over edges.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public abstract class EdgeIterator implements Iterator<long[]>, Iterable<long[]>, AutoCloseable {

  protected long[] next;

  @Override
  public Iterator<long[]> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public long[] next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    long[] n = next;
    next = getNext();
    return n;
  }

  protected abstract long[] getNext();

  @Override
  public abstract void close();

}
