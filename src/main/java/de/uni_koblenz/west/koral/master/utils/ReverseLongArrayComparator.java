package de.uni_koblenz.west.koral.master.utils;

import java.util.Comparator;

/**
 * Compars long arrays in inverse lexicographic order.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class ReverseLongArrayComparator implements Comparator<long[]> {

  private final boolean ascendingOrder;

  public ReverseLongArrayComparator(boolean ascendingOrder) {
    this.ascendingOrder = ascendingOrder;
  }

  @Override
  public int compare(long[] o1, long[] o2) {
    return ascendingOrder ? internalCompare(o1, o2) : -internalCompare(o1, o2);
  }

  private int internalCompare(long[] o1, long[] o2) {
    int minLength = o1.length < o2.length ? o1.length : o2.length;
    for (int i = minLength - 1; i >= 0; i--) {
      if (o1[i] < o2[i]) {
        return -1;
      } else if (o1[i] > o2[i]) {
        return 1;
      }
    }
    if (o1.length < o2.length) {
      return -1;
    } else if (o1.length > o2.length) {
      return 1;
    } else {
      return 0;
    }
  }

}
