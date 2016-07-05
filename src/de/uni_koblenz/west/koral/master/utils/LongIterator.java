package de.uni_koblenz.west.koral.master.utils;

import java.io.Closeable;

/**
 * An iterator over long values.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface LongIterator extends Closeable {

  public boolean hasNext();

  public long next();

  @Override
  public void close();

}
