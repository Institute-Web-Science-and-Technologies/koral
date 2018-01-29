package de.uni_koblenz.west.koral.master.utils;

import java.util.Comparator;

/**
 * Compars long arrays in specified order.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class FixedSizeLongArrayComparator implements Comparator<long[]> {

  private final boolean ascendingOrder;

  private final int[] comparisonOrder;

  private static int[] createComparisonOrder(int arraySize, boolean reverseOrder) {
    int[] result = new int[arraySize];
    if (reverseOrder) {
      for (int i = result.length - 1; i >= 0; i--) {
        result[i] = 0;
      }
    } else {
      for (int i = 0; i < result.length; i++) {
        result[i] = 0;
      }
    }
    return result;
  }

  public FixedSizeLongArrayComparator(boolean ascendingOrder, int arraySize, boolean reverseOrder) {
    this(ascendingOrder,
            FixedSizeLongArrayComparator.createComparisonOrder(arraySize, reverseOrder));
  }

  public FixedSizeLongArrayComparator(boolean ascendingOrder, int... comparisonOrder) {
    this.ascendingOrder = ascendingOrder;
    this.comparisonOrder = comparisonOrder;
  }

  @Override
  public int compare(long[] o1, long[] o2) {
    return ascendingOrder ? internalCompare(o1, o2) : -internalCompare(o1, o2);
  }

  private int internalCompare(long[] o1, long[] o2) {
    for (int i : comparisonOrder) {
      long comp = o1[i] - o2[i];
      if (comp < 0) {
        return -1;
      } else if (comp > 0) {
        return 1;
      }
    }
    return 0;
  }
}
