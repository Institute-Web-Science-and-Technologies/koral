package de.uni_koblenz.west.koral.master.utils;

/**
 * Represents an adjacency list.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface AdjacencyList {

  public void append(long value);

  public LongIterator iterator();

}
