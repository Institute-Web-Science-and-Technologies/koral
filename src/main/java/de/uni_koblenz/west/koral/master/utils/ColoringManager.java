package de.uni_koblenz.west.koral.master.utils;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Keeps track of the coloring of the edges.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class ColoringManager implements AutoCloseable {

  private File edge2colorFolder;

  private final Map<Long, Long> edge2color;

  private File colorsFolder;

  /**
   * value % 2 == 1, color was recolored<br>
   * value % 2 == 0, size of color<br>
   * the actual colorId is retrieved by value&gt;&gt;&gt;1
   */
  private final Map<Long, Long> colors;

  private long nextColor;

  private long numberOfColors;

  private final long[] internalEdgeInfo;

  public ColoringManager(File internalWorkingDir, int numberOfOpenFiles) {
    edge2color = new HashMap<>();
    colors = new HashMap<>();
    nextColor = 1;
    internalEdgeInfo = new long[2];
    numberOfColors = 0;
  }

  /**
   * @param edges[i][1]==0
   *          iff edge is uncolored
   */
  public void fillColorInformation(long[][] edges) {
    fillColorInformation(edges, edges.length);
  }

  /**
   * @param edges
   *          edges[i][0]=edgeId, edges[i][1]=colorId, edges[i][2]=colorSize
   *          (the latter two will be filled)
   * @param lastExclusiveIndex
   *          last index (exclusive) to which the array is filled
   */
  public void fillColorInformation(long[][] edges, int lastExclusiveIndex) {
    for (int i = 0; i < lastExclusiveIndex; i++) {
      fillEdgeColor(edges[i][0], edges[i], 1);
    }
  }

  private void fillEdgeColor(long edge, long[] colorArray, int startIndex) {
    Long color = edge2color.get(edge);
    if (color == null) {
      colorArray[startIndex] = 0;
      colorArray[startIndex + 1] = 0;
    } else {
      long[] colorInfo = getColor(color);
      if (colorInfo == null) {
        throw new RuntimeException("The color c" + color + " is unknown.");
      }
      colorArray[startIndex] = colorInfo[0];
      colorArray[startIndex + 1] = colorInfo[1];
    }
  }

  private long[] getColor(Long colorId) {
    Long size = colors.get(colorId);
    if (size == null) {
      return null;
    }
    Long previousColor = null;
    Set<Long> recoloredColors = new HashSet<>();
    while ((size.longValue() & 0x01L) == 1) {
      // the color was recolored
      previousColor = colorId;
      colorId = size.longValue() >>> 1;
      size = colors.get(colorId);
      if (size == null) {
        throw new RuntimeException("The recolored color c" + colorId + " is unknown.");
      }
      if ((size.longValue() & 0x01L) == 1) {
        recoloredColors.add(previousColor);
      }
    }
    // shorten the search path for recolored edges,
    // i.e., c1->c2->c3->c4 is shortened to c1->c4; c2->c4; c3->c4
    for (Long recoloredColor : recoloredColors) {
      recolorColor(recoloredColor, colorId);
    }
    long[] color = internalEdgeInfo;
    internalEdgeInfo[0] = colorId;
    internalEdgeInfo[1] = size >>> 1;
    return color;
  }

  /**
   * A new array must be returned
   * 
   * @return color[0]=colorId; color[1]=frequency of color
   */
  public long[] createNewColor() {
    long newColorId = nextColor++;
    setColor(newColorId, 0);
    numberOfColors++;
    return new long[] { newColorId, 0 };
  }

  private void setColor(long newColor, long size) {
    if (newColor == 0) {
      throw new RuntimeException("Attempt to create color c0.");
    }
    colors.put(newColor, size << 1);
  }

  // TODO remove
  public long edges;

  public void colorEdge(long edge, long colorId) {
    if (colorId == 0) {
      throw new RuntimeException("Attempt to set color of edge e" + edge + " to c" + colorId + ".");
    }
    // TODO remove
    long[][] e = new long[][] { { edge, 0, 0 } };
    fillColorInformation(e);
    if ((e[0][1] != colorId) && (e[0][1] != 0)) {
      throw new RuntimeException("Edge e" + edge + " should be colored in c" + colorId
              + " but has already color c" + e[0][1] + ".");
    }
    edges++;
    // System.out.println(">>>>color e" + edge + "->c" + colorId);
    long[] color = getColor(colorId);
    if (color[0] != colorId) {
      throw new RuntimeException("Attempt to color edge e" + edge + " in the color c" + colorId
              + " which was already recolored to c" + color[0]);
    }
    setEdgeColor(edge, color[0]);
    setColor(color[0], color[1] + 1);
    // TODO remove
    // System.out.println(this);
  }

  private void setEdgeColor(long edge, long color) {
    if (color == 0) {
      throw new RuntimeException("Attempt to set color of edge e" + edge + " to c" + color + ".");
    }
    edge2color.put(edge, color);
  }

  public void recolor(long oldColor, long oldColorSize, long newColor, long newColorSize) {
    // TODO remove
    // System.out.println(">>>>recolor c" + oldColor + "->c" + newColor);
    if (oldColor == 0) {
      throw new RuntimeException("Attempt to recolor color c" + oldColor + ".");
    }
    if (newColor == 0) {
      throw new RuntimeException("Attempt to recolor color c" + oldColor + " to color c0.");
    }
    // TODO remove
    long ocSize = getColor(oldColor)[1];
    if (ocSize != oldColorSize) {
      throw new RuntimeException("Color c" + oldColor + " should have a size of " + oldColorSize
              + " but actually has a size of " + ocSize);
    }
    ocSize = getColor(newColor)[1];
    if (ocSize != newColorSize) {
      throw new RuntimeException("Color c" + newColor + " should have a size of " + newColorSize
              + " but actually has a size of " + ocSize);
    }
    recolorColor(oldColor, newColor);
    setColor(newColor, oldColorSize + newColorSize);
    numberOfColors--;
    // TODO remove
    // System.out.println(colors);
    // System.out.println(this);
  }

  private void recolorColor(long oldColor, long newColor) {
    if (oldColor == 0) {
      throw new RuntimeException("Attempt to recolor color c" + oldColor + ".");
    }
    if (newColor == 0) {
      throw new RuntimeException("Attempt to recolor color c" + oldColor + " to color c0.");
    }
    colors.put(oldColor, (newColor << 1) | 0x01L);
  }

  public long getNumberOfColors() {
    return numberOfColors;
  }

  /**
   * 
   * @return Iterator over all colors. Each color consists of: color[0]=colorId;
   *         color[1]=frequency of color
   */
  public Iterator<long[]> getIteratorOverAllColors() {
    return new Iterator<long[]>() {

      private final Iterator<Entry<Long, Long>> iterator = colors.entrySet().iterator();

      private long[] next = getNext();

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

      private long[] getNext() {
        long color = 0;
        long size = 1;
        while (iterator.hasNext()) {
          Entry<Long, Long> entry = iterator.next();
          color = entry.getKey();
          size = entry.getValue();
          if ((size & 0x01L) == 0) {
            size = size >>> 1;
            return new long[] { color, size };
          }
        }
        return null;
      }
    };
  }

  /**
   * @return Iterator over long[]{edgeId, colorId}
   */
  public Iterator<long[]> getIteratorOverColoredEdges() {
    // TODO remove
    System.out.println(">>>>edges called: " + edges);
    System.out.println(">>>>edges known in color manager: " + edge2color.size());
    return new Iterator<long[]>() {

      private final Iterator<Entry<Long, Long>> iterator = edge2color.entrySet().iterator();

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public long[] next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        Entry<Long, Long> element = iterator.next();
        long[] color = getColor(element.getValue());
        return new long[] { element.getKey(), color[0] };
      }
    };
  }

  @Override
  public void close() {
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ColorManager:\n");
    sb.append("\tedges: {");
    String delim = "";
    Iterator<long[]> iterator = getIteratorOverColoredEdges();
    while (iterator.hasNext()) {
      long[] edgeColor = iterator.next();
      sb.append(delim).append("(e").append(edgeColor[0]).append(",c").append(edgeColor[1])
              .append(")");
      delim = ", ";
    }
    sb.append("}");
    sb.append("\n");
    delim = "\tcolors: {";
    iterator = getIteratorOverAllColors();
    while (iterator.hasNext()) {
      long[] color = iterator.next();
      sb.append(delim).append("(c").append(color[0]).append(",#").append(color[1]).append(")");
      delim = ", ";
    }
    sb.append("}");
    return sb.toString();
  }

}
