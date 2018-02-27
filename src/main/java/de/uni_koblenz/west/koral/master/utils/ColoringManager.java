package de.uni_koblenz.west.koral.master.utils;

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Keeps track of the coloring of the edges.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class ColoringManager implements AutoCloseable {

  private final SimpleLongMap edge2color;

  /**
   * value % 2 == 1, color was recolored<br>
   * value % 2 == 0, size of color<br>
   * the actual colorId is retrieved by value&gt;&gt;&gt;1
   */
  private final SimpleLongMap colors;

  private long nextColor;

  private long numberOfColors;

  private final long[] internalEdgeInfo;

  public ColoringManager(File internalWorkingDir, int numberOfOpenFiles) {
    // edge2color = new RocksDBSimpleLongMap(
    // new File(internalWorkingDir.getAbsolutePath() + File.separator +
    // "edge2colorMap"),
    // numberOfOpenFiles / 2);
    // colors = new RocksDBSimpleLongMap(
    // new File(internalWorkingDir.getAbsolutePath() + File.separator +
    // "colorsMap"),
    // numberOfOpenFiles / 2);
    edge2color = new SingleFileSimpleLongMap(
            new File(internalWorkingDir.getAbsolutePath() + File.separator + "edge2colorMap"),
            false);
    colors = new SingleFileSimpleLongMap(
            new File(internalWorkingDir.getAbsolutePath() + File.separator + "colorsMap"), true);
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

  public void fillEdgeColor(long edge, long[] colorArray, int startIndex) {
    try {
      long color = edge2color.get(edge);
      long[] colorInfo = getColor(color);
      if (colorInfo == null) {
        throw new RuntimeException("The color c" + color + " is unknown.");
      }
      colorArray[startIndex] = colorInfo[0];
      colorArray[startIndex + 1] = colorInfo[1];
    } catch (NoSuchElementException e) {
      colorArray[startIndex] = 0;
      colorArray[startIndex + 1] = 0;
    }
  }

  private long[] getColor(long colorId) {
    try {
      long size = colors.get(colorId);
      long previousColor = 0;
      Set<Long> recoloredColors = new HashSet<>();
      while ((size & 0x01L) == 1) {
        // the color was recolored
        previousColor = colorId;
        colorId = size >>> 1;
        try {
          size = colors.get(colorId);
        } catch (NoSuchElementException e) {
          throw new RuntimeException("The recolored color c" + colorId + " is unknown.");
        }
        if ((size & 0x01L) == 1) {
          recoloredColors.add(previousColor);
        }
      }
      // shorten the search path for recolored edges,
      // i.e., c1->c2->c3->c4 is shortened to c1->c4; c2->c4; c3->c4
      for (Long recoloredColor : recoloredColors) {
        recolorColor(recoloredColor, colorId);
      }
      long[] color = internalEdgeInfo;
      color[0] = colorId;
      color[1] = size >>> 1;
      return color;
    } catch (NoSuchElementException e) {
      return null;
    }
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

  public void colorEdge(long edge, long colorId) {
    if (colorId == 0) {
      throw new RuntimeException("Attempt to set color of edge e" + edge + " to c" + colorId + ".");
    }
    // long[][] e = new long[][] { { edge, 0, 0 } };
    // fillColorInformation(e);
    // if ((e[0][1] != colorId) && (e[0][1] != 0)) {
    // throw new RuntimeException("Edge e" + edge + " should be colored in c" +
    // colorId
    // + " but has already color c" + e[0][1] + ".");
    // }
    long[] color = getColor(colorId);
    if (color[0] != colorId) {
      throw new RuntimeException("Attempt to color edge e" + edge + " in the color c" + colorId
              + " which was already recolored to c" + color[0]);
    }
    setEdgeColor(edge, color[0]);
    setColor(color[0], color[1] + 1);
  }

  public void changeColor(long edge, long oldColorId, long newColorId) {
    if (oldColorId == 0) {
      throw new RuntimeException(
              "Attempt to change color of edge e" + edge + " from c" + oldColorId + ".");
    }
    if (newColorId == 0) {
      throw new RuntimeException(
              "Attempt to change color of edge e" + edge + " to c" + newColorId + ".");
    }
    long[] oldColor = getColor(oldColorId);
    oldColor[1]--;
    setColor(oldColor[0], oldColor[1]);
    if (oldColor[1] == 0) {
      numberOfColors--;
    }
    long[] newColor = getColor(newColorId);
    setEdgeColor(edge, newColor[0]);
    setColor(newColor[0], newColor[1] + 1);
  }

  private void setEdgeColor(long edge, long color) {
    if (color == 0) {
      throw new RuntimeException("Attempt to set color of edge e" + edge + " to c" + color + ".");
    }
    edge2color.put(edge, color);
  }

  public void recolor(long oldColor, long oldColorSize, long newColor, long newColorSize) {
    if (oldColor == newColor) {
      throw new RuntimeException(
              "Attempt to recolor color c" + oldColor + " to color c" + newColor + ".");
    }
    if (oldColor == 0) {
      throw new RuntimeException("Attempt to recolor color c" + oldColor + ".");
    }
    if (newColor == 0) {
      throw new RuntimeException("Attempt to recolor color c" + oldColor + " to color c0.");
    }
    long newSize = 0;
    long[] oldColorInfo = getColor(oldColor);
    if (oldColorInfo[0] != oldColor) {
      throw new RuntimeException(
              "Old color c" + oldColor + " was already recolored to " + oldColorInfo[0]);
    }
    newSize = oldColorInfo[1];
    long[] newColorInfo = getColor(newColor);
    if (newColorInfo[0] != newColor) {
      throw new RuntimeException(
              "New color c" + newColor + " was already recolored to " + newColorInfo[0]);
    }
    newSize += newColorInfo[1];
    long newColor2 = newColorInfo[0];
    recolorColor(oldColor, newColor2);
    setColor(newColor2, newSize);
    numberOfColors--;
  }

  private void recolorColor(long recoloredColor, long colorId) {
    if (recoloredColor == 0) {
      throw new RuntimeException("Attempt to recolor color c" + recoloredColor + ".");
    }
    if (colorId == 0) {
      throw new RuntimeException("Attempt to recolor color c" + recoloredColor + " to color c0.");
    }
    colors.put(recoloredColor, (colorId << 1) | 0x01L);
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
    flush();
    return new Iterator<long[]>() {

      private final Iterator<long[]> iterator = colors.iterator();

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
        long[] next = null;
        while (iterator.hasNext()) {
          next = iterator.next();
          if ((next[1] != 0) && ((next[1] & 0x01L) == 0)) {
            return new long[] { next[0], next[1] >>> 1 };
          }
        }
        return null;
      }
    };
  }

  private void flush() {
    edge2color.flush();
    colors.flush();
  }

  /**
   * @return Iterator over long[]{edgeId, colorId}
   */
  public Iterator<long[]> getIteratorOverColoredEdges() {
    flush();
    return new Iterator<long[]>() {

      private final Iterator<long[]> iterator = edge2color.iterator();

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
        if (!iterator.hasNext()) {
          return null;
        }
        long[] n = iterator.next();
        return new long[] { n[0], getColor(n[1])[0] };
      }
    };
  }

  @Override
  public void close() {
    edge2color.close();
    colors.close();
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
