package de.uni_koblenz.west.koral.master.utils;

/**
 * 
 * Iterates over all edges stored in an array.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class EdgeArrayIterator extends EdgeIterator {

  private long[][] edges;

  private int nextIndex;

  public EdgeArrayIterator(long[][] edges) {
    this.edges = edges;
    nextIndex = 0;
    next = getNext();
  }

  @Override
  protected long[] getNext() {
    if (nextIndex >= edges.length) {
      return null;
    } else {
      return edges[nextIndex++];
    }
  }

  @Override
  public void close() {
    edges = null;
  }

}
