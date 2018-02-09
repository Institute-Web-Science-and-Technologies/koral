package de.uni_koblenz.west.koral.master.utils;

import de.uni_koblenz.west.koral.common.utils.ReusableIDGenerator;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

/**
 * Keeps track of the coloring of the edges.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class ColoringManager implements AutoCloseable {

  private File edge2offsetFolder;

  private final Map<Long, Long> edge2offset;

  private File offset2colorFolder;

  /**
   * offset values must have first bit set to 1 in the value position
   */
  private final Map<Long, Long> offset2color;

  private long nextOffset;

  private File colorsFolder;

  /**
   * colorIds must have the first bit = 0
   * 
   * value[0] = colorSize, value[1]=offset
   */
  private final Map<Long, long[]> colors;

  private final ReusableIDGenerator colorIdGenerator;

  private long numberOfColors;

  // FIXME reimplement without offset index
  // reuse color id
  // repaint will set the color array to oldColor->[newColor]

  public ColoringManager(File internalWorkingDir, int numberOfOpenFiles) {
    edge2offset = new HashMap<>();
    offset2color = new HashMap<>();
    nextOffset = 1;
    colors = new HashMap<>();
    colorIdGenerator = new ReusableIDGenerator();
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
      edges[i][1] = getEdgeColor(edges[i][0]);
      edges[i][2] = getColorInformation(edges[i][1])[0];
      // TODO remove
      if (edges[i][0] == 12) {
        // FIXME
        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n" + Arrays.toString(edges[i]));
        System.out.println("edge e" + edges[i][0]);
        System.out.println("offset " + getOffsetFromEdge(edges[i][0]));
        System.out.println("colorID " + getColorId(getOffsetFromEdge(edges[i][0])));
        System.out.println("color information "
                + Arrays.toString(getColorInformation(getColorId(getOffsetFromEdge(edges[i][0])))));
        System.out.println("edge2offset " + edge2offset.get(edges[i][0]));
        System.out.println("offset2color " + offset2color.get(getOffsetFromEdge(edges[i][0])));
        System.out.println("colors "
                + Arrays.toString(colors.get(getColorId(getOffsetFromEdge(edges[i][0])))));
      }
    }
  }

  private long getEdgeColor(long edge) {
    long offset = getOffsetFromEdge(edge);
    long colorId = getColorId(offset);
    return colorId;
  }

  private long getOffsetFromEdge(long edge) {
    Long offset = edge2offset.get(edge);
    if (offset == null) {
      return 0;
    } else {
      return offset;
    }
  }

  private long getOffsetFromColor(long colorId) {
    long[] colorInfo = colors.get(colorId);
    if (colorInfo == null) {
      return 0;
    } else {
      return colorInfo[1];
    }
  }

  private long getColorId(long offset) {
    Long colorId = offset2color.get(offset);
    while ((colorId != null) && (colorId < 0)) {
      // the value of this map is negative if it is another offset. If it is a
      // color, then it is positive
      // the key of the map is always positive
      colorId = offset2color.get(colorId & 0x7f_ff_ff_ff_ff_ff_ff_ffL);
    }
    return colorId == null ? 0 : colorId;
  }

  private long[] getColorInformation(long color) {
    long[] colorInfos = colors.get(color);
    if (colorInfos == null) {
      return new long[2];
    } else {
      return Arrays.copyOf(colorInfos, colorInfos.length);
    }
  }

  /**
   * A new array must be returned
   * 
   * @return color[0]=colorId; color[1]=frequency of color
   */
  public long[] createNewColor() {
    long newColorId = colorIdGenerator.getNextId() + 1;
    long newOffset = nextOffset++;
    setOffsetInformation(newOffset, newColorId);
    setColorInformation(newColorId, 0, newOffset);
    numberOfColors++;
    return new long[] { newColorId, 0 };
  }

  private void setOffsetInformation(long offset, long colorId) {
    if (offset == 0) {
      throw new RuntimeException("Try to set offset 0 to color c" + colorId + "!");
    }
    offset2color.put(offset, colorId);
  }

  private void setColorInformation(long colorId, long size, long offset) {
    if (colorId <= 0) {
      throw new RuntimeException("Try to set information for color c" + offset + ".");
    }
    long[] colorInfo = colors.get(colorId);
    if (colorInfo == null) {
      colorInfo = new long[] { size, offset };
    } else {
      colorInfo[0] = size;
    }
    colors.put(colorId, colorInfo);
  }

  private void setEdgeColor(long edge, long offset) {
    // TODO remove
    Long oldOffset = edge2offset.get(edge);
    if (oldOffset != null) {
      throw new RuntimeException("edge " + edge + " has already the offset " + oldOffset
              + " instead of the new offset " + offset + ".");
    }
    edge2offset.put(edge, offset);
  }

  private void deleteColor(long oldColor) {
    colors.remove(oldColor);
    colorIdGenerator.release(oldColor - 1);
    numberOfColors--;
  }

  public long edges;

  public void colorEdge(long edge, long colorId) {
    edges++;
    // TODO remove
    // System.out.println(">>>>color e" + edge + "->c" + colorId);
    long[] color = getColorInformation(colorId);
    setEdgeColor(edge, color[1]);
    setColorInformation(colorId, color[0] + 1, color[1]);
    // TODO remove
    // System.out.println(this);
  }

  public void recolor(long oldColor, long oldColorSize, long newColor, long newColorSize) {
    // TODO remove
    // System.out.println(">>>>recolor c" + oldColor + "->c" + newColor);
    long oldColorOffset = getOffsetFromColor(oldColor);
    if (oldColorOffset == 0) {
      throw new RuntimeException("Old color c" + oldColor + " is unknown.");
    }
    long newColorOffset = getOffsetFromColor(newColor);
    if (newColorOffset == 0) {
      throw new RuntimeException("New color c" + newColor + " is unknown.");
    }
    setOffsetInformation(oldColorOffset, newColorOffset | 0x80_00_00_00_00_00_00_00L);
    setColorInformation(newColor, oldColorSize + newColorSize, 0);
    // deleteColor(oldColor);
    // TODO remove
    // System.out.println(this);
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

      private final Iterator<Entry<Long, long[]>> iterator = colors.entrySet().iterator();

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public long[] next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        Entry<Long, long[]> element = iterator.next();
        return new long[] { element.getKey(), element.getValue()[0] };
      }
    };
  }

  /**
   * @return Iterator over long[]{edgeId, colorId}
   */
  public Iterator<long[]> getIteratorOverColoredEdges() {
    // TODO remove
    System.out.println(">>>>edges called: " + edges);
    System.out.println(">>>>edges known in color manager: " + edge2offset.size());
    return new Iterator<long[]>() {

      private final Iterator<Entry<Long, Long>> iterator = edge2offset.entrySet().iterator();

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
        return new long[] { element.getKey(), getColorId(element.getValue()) };
      }
    };
  }

  @Override
  public void close() {
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ColorManager:\n");
    String delim = "\tedges: {";
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
