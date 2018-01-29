package de.uni_koblenz.west.koral.master.utils;

import java.util.Collections;
import java.util.Comparator;

/**
 * Compars two long arrays using only a single field for comparison. It cannot
 * be used in {@link Collections}!
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class LongArraySingleFieldComparator implements Comparator<long[]> {

  private static final LongArraySingleFieldComparator[] comparators = new LongArraySingleFieldComparator[2];

  private final boolean ascendingOrder;

  private final int index;

  public LongArraySingleFieldComparator(boolean ascendingOrder, int comparisonIndex) {
    this.ascendingOrder = ascendingOrder;
    index = comparisonIndex;
  }

  @Override
  public int compare(long[] o1, long[] o2) {
    return ascendingOrder ? internalCompare(o1, o2) : -internalCompare(o1, o2);
  }

  private int internalCompare(long[] o1, long[] o2) {
    long degreeComparison = o1[index] - o2[index];
    if (degreeComparison < 0) {
      return -1;
    }
    if (degreeComparison > 0) {
      return 1;
    } else {
      return 0;
    }
  }

  public static LongArraySingleFieldComparator getVertexIdComparator(boolean ascendingOrder) {
    if (LongArraySingleFieldComparator.comparators[ascendingOrder ? 0 : 1] == null) {
      LongArraySingleFieldComparator.comparators[ascendingOrder ? 0
              : 1] = new LongArraySingleFieldComparator(ascendingOrder, 0);
    }
    return LongArraySingleFieldComparator.comparators[ascendingOrder ? 0 : 1];
  }

}
