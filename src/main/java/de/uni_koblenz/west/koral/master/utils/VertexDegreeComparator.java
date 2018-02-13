package de.uni_koblenz.west.koral.master.utils;

import java.util.Comparator;

/**
 * Compares two arrays and adds out degree and in degree.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class VertexDegreeComparator implements Comparator<long[]> {

  private static final VertexDegreeComparator[] comparators = new VertexDegreeComparator[2];

  private final boolean ascendingOrder;

  private VertexDegreeComparator(boolean ascendingOrder) {
    this.ascendingOrder = ascendingOrder;
  }

  @Override
  public int compare(long[] o1, long[] o2) {
    return ascendingOrder ? internalCompare(o1, o2) : -internalCompare(o1, o2);
  }

  private int internalCompare(long[] o1, long[] o2) {
    long degreeComparison = (o1[0] + o1[1]) - (o2[0] + o2[1]);
    if (degreeComparison < 0) {
      return -1;
    }
    if (degreeComparison > 0) {
      return 1;
    } else {
      return 0;
    }
  }

  public static VertexDegreeComparator getComparator(boolean ascendingOrder) {
    if (VertexDegreeComparator.comparators[ascendingOrder ? 0 : 1] == null) {
      VertexDegreeComparator.comparators[ascendingOrder ? 0 : 1] = new VertexDegreeComparator(
              ascendingOrder);
    }
    return VertexDegreeComparator.comparators[ascendingOrder ? 0 : 1];
  }

}
