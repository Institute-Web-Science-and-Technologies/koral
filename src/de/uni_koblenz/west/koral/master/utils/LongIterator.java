package de.uni_koblenz.west.koral.master.utils;

/**
 * An iterator over long values.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface LongIterator extends AutoCloseable {

  public boolean hasNext();

  public long next();

  @Override
  public void close();

}
