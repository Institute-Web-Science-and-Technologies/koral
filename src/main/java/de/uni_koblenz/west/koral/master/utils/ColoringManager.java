package de.uni_koblenz.west.koral.master.utils;

import java.io.File;
import java.util.Iterator;

/**
 * Keeps track of the coloring of the edges.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class ColoringManager implements AutoCloseable {

  public ColoringManager(File internalWorkingDir, int numberOfOpenFiles) {
    // TODO Auto-generated constructor stub
  }

  /**
   * @param edges[i][1]==0
   *          iff edge is uncolored
   */
  public void fillColorInformation(long[][] edges) {
    fillColorInformation(edges, edges.length);
  }

  /**
   * @param cachedEdges
   * @param nextIndex
   *          last index (exclusive) to which the array is filled
   */
  public void fillColorInformation(long[][] edges, int nextIndex) {
    // TODO Auto-generated method stub

  }

  public long[] getColorInformation(long edge) {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * A new array must be returned
   * 
   * @return color[0]=colorId; color[1]=frequency of color
   */
  public long[] createNewColor() {
    // TODO Auto-generated method stub
    return null;
  }

  public void colorEdge(long edge, long colorId) {
    // TODO Auto-generated method stub

  }

  public void recolor(long oldColor, long oldColorSize, long newColor, long newColorSize) {
    // TODO Auto-generated method stub

  }

  public long getNumberOfColors() {
    // TODO Auto-generated method stub
    return 0;
  }

  /**
   * 
   * @return Iterator over all colors. Each color consists of: color[0]=colorId;
   *         color[1]=frequency of color
   */
  public Iterator<long[]> getIteratorOverAllColors() {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * @return Iterator over long[]{edgeId, colorId}
   */
  public Iterator<long[]> getIteratorOverColoredEdges() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

}
