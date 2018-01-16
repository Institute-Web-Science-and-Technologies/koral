package de.uni_koblenz.west.koral.master.utils;

import java.util.Comparator;

/**
 * Compars two arrays of {@link VertexIncidencentEdgesListFileCreator}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class VertexIncidentEdgesVertexIdComparator implements Comparator<long[]> {

  private static final VertexIncidentEdgesVertexIdComparator[] comparators = new VertexIncidentEdgesVertexIdComparator[2];

  private final boolean ascendingOrder;

  private VertexIncidentEdgesVertexIdComparator(boolean ascendingOrder) {
    this.ascendingOrder = ascendingOrder;
  }

  @Override
  public int compare(long[] o1, long[] o2) {
    return ascendingOrder ? internalCompare(o1, o2) : -internalCompare(o1, o2);
  }

  private int internalCompare(long[] o1, long[] o2) {
    long degreeComparison = o1[0] - o2[0];
    if (degreeComparison < 0) {
      return -1;
    }
    if (degreeComparison > 0) {
      return 1;
    } else {
      return 0;
    }
  }

  public static VertexIncidentEdgesVertexIdComparator getComparator(boolean ascendingOrder) {
    if (VertexIncidentEdgesVertexIdComparator.comparators[ascendingOrder ? 0 : 1] == null) {
      VertexIncidentEdgesVertexIdComparator.comparators[ascendingOrder ? 0
              : 1] = new VertexIncidentEdgesVertexIdComparator(ascendingOrder);
    }
    return VertexIncidentEdgesVertexIdComparator.comparators[ascendingOrder ? 0 : 1];
  }

}
