package de.uni_koblenz.west.koral.master.utils;

import java.util.Comparator;

/**
 * Compars two arrays of {@link VertexIncidencentEdgesListFileCreator}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class VertexIncidentEdgesComparator implements Comparator<long[]> {

  private static final VertexIncidentEdgesComparator[] comparators = new VertexIncidentEdgesComparator[2];

  private final boolean ascendingOrder;

  private VertexIncidentEdgesComparator(boolean ascendingOrder) {
    this.ascendingOrder = ascendingOrder;
  }

  @Override
  public int compare(long[] o1, long[] o2) {
    return ascendingOrder ? internalCompare(o1, o2) : -internalCompare(o1, o2);
  }

  private int internalCompare(long[] o1, long[] o2) {
    long degreeComparison = (o1[1] + o1[2]) - (o2[1] + o2[2]);
    if (degreeComparison < 0) {
      return -1;
    }
    if (degreeComparison > 0) {
      return 1;
    } else {
      if (o1[0] < o2[0]) {
        return -1;
      } else if (o1[0] > o2[0]) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  public static VertexIncidentEdgesComparator getComparator(boolean ascendingOrder) {
    if (VertexIncidentEdgesComparator.comparators[ascendingOrder ? 0 : 1] == null) {
      VertexIncidentEdgesComparator.comparators[ascendingOrder ? 0
              : 1] = new VertexIncidentEdgesComparator(ascendingOrder);
    }
    return VertexIncidentEdgesComparator.comparators[ascendingOrder ? 0 : 1];
  }

}
