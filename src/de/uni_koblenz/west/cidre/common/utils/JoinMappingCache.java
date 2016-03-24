package de.uni_koblenz.west.cidre.common.utils;

import de.uni_koblenz.west.cidre.common.query.Mapping;

import java.io.Closeable;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Instances are used for joins. It caches the mappings received from one
 * specific child of the join operation.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface JoinMappingCache extends Closeable, Iterable<Mapping> {

  public boolean isEmpty();

  public long size();

  public void add(Mapping mapping);

  public Iterator<Mapping> getMatchCandidates(Mapping mapping, long[] mappingVars);

  @Override
  public void close();

  public static class JoinComparator implements Comparator<byte[]>, Serializable {

    private static final long serialVersionUID = -7360345226100972052L;

    private final int offset = Mapping.getHeaderSize();

    private final int[] comparisonOrder;

    /**
     * ComparisonOrder must contain all variables of the mapping. First variable
     * has index 0.
     * 
     * @param comparisonOrder
     */
    public JoinComparator(int[] comparisonOrder) {
      this.comparisonOrder = comparisonOrder;
    }

    @Override
    public int compare(byte[] thisMapping, byte[] otherMapping) {
      if (thisMapping == otherMapping) {
        return 0;
      }
      for (int var : comparisonOrder) {
        int comparison = longCompare(getVar(var, thisMapping), getVar(var, otherMapping));
        if (comparison != 0) {
          return comparison;
        }
      }
      // compare containment information
      final int len = Math.min(thisMapping.length, otherMapping.length);
      for (int i = offset + comparisonOrder.length * Long.BYTES; i < len; i++) {
        if (thisMapping[i] < otherMapping[i]) {
          return -1;
        }
        if (thisMapping[i] > otherMapping[i]) {
          return 1;
        }
      }
      int comparison = intCompare(thisMapping.length, otherMapping.length);
      return comparison;
    }

    private long getVar(int varIndex, byte[] mapping) {
      return NumberConversion.bytes2long(mapping, offset + varIndex * Long.BYTES);
    }

    private int intCompare(int x, int y) {
      return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    private int longCompare(long x, long y) {
      return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

  }

}
