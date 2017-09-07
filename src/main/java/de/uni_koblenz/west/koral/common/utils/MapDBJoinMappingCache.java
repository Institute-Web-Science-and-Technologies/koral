/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License
 * along with Koral.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
package de.uni_koblenz.west.koral.common.utils;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.query.Mapping;
import de.uni_koblenz.west.koral.common.query.MappingRecycleCache;

import java.io.File;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;

/**
 * Instances are used for joins. It caches the mappings received from one
 * specific child of the join operation.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MapDBJoinMappingCache implements JoinMappingCache {

  private final MappingRecycleCache recycleCache;

  private final File mapFolder;

  private final DB database;

  private final NavigableSet<byte[]> multiMap;

  private final long[] variables;

  private final int[] joinVarIndices;

  private int size;

  /**
   * @param storageType
   * @param useTransactions
   * @param writeAsynchronously
   * @param cacheType
   * @param cacheDirectory
   * @param recycleCache
   * @param uniqueFileNameSuffix
   * @param mappingVariables
   * @param variableComparisonOrder
   *          must contain all variables of the mapping. First variable has
   *          index 0. The join variables must occur first!
   * @param numberOfJoinVars
   */
  public MapDBJoinMappingCache(MapDBStorageOptions storageType, boolean useTransactions,
          boolean writeAsynchronously, MapDBCacheOptions cacheType, File cacheDirectory,
          MappingRecycleCache recycleCache, String uniqueFileNameSuffix, long[] mappingVariables,
          int[] variableComparisonOrder, int numberOfJoinVars) {
    assert (storageType != MapDBStorageOptions.MEMORY) || (cacheDirectory != null);
    this.recycleCache = recycleCache;
    variables = mappingVariables;
    joinVarIndices = new int[numberOfJoinVars];
    for (int i = 0; i < numberOfJoinVars; i++) {
      joinVarIndices[i] = variableComparisonOrder[i];
    }
    mapFolder = new File(cacheDirectory.getAbsolutePath() + File.separator + uniqueFileNameSuffix);
    if (!mapFolder.exists()) {
      mapFolder.mkdirs();
    }
    DBMaker<?> dbmaker = storageType
            .getDBMaker(mapFolder.getAbsolutePath() + File.separator + uniqueFileNameSuffix);
    if (!useTransactions) {
      dbmaker = dbmaker.transactionDisable().closeOnJvmShutdown();
    }
    if (writeAsynchronously) {
      dbmaker = dbmaker.asyncWriteEnable();
    }
    dbmaker = cacheType.setCaching(dbmaker);
    database = dbmaker.make();

    multiMap = database.createTreeSet(uniqueFileNameSuffix)
            .comparator(new JoinComparator(variableComparisonOrder)).makeOrGet();

    size = 0;
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public synchronized void add(Mapping mapping) {
    if (database.isClosed()) {
      throw new RuntimeException("Adding a mapping not possible because the "
              + MapDBJoinMappingCache.class.getSimpleName() + " is already closed.");
    }
    size++;
    byte[] newMapping = new byte[mapping.getLengthOfMappingInByteArray()];
    System.arraycopy(mapping.getByteArray(), mapping.getFirstIndexOfMappingInByteArray(),
            newMapping, 0, mapping.getLengthOfMappingInByteArray());
    multiMap.add(newMapping);
  }

  @Override
  public synchronized Iterator<Mapping> getMatchCandidates(Mapping mapping, long[] mappingVars) {
    int headerSize = Mapping.getHeaderSize();
    byte[] min = new byte[headerSize + (variables.length * Long.BYTES)
            + mapping.getNumberOfContainmentBytes()];
    byte[] max = new byte[headerSize + (variables.length * Long.BYTES)
            + mapping.getNumberOfContainmentBytes()];
    // set join vars
    for (int varIndex : joinVarIndices) {
      NumberConversion.long2bytes(mapping.getValue(variables[varIndex], mappingVars), min,
              headerSize + (varIndex * Long.BYTES));
      NumberConversion.long2bytes(mapping.getValue(variables[varIndex], mappingVars), max,
              headerSize + (varIndex * Long.BYTES));
    }
    // set non join vars
    for (int i = 0; i < min.length; i++) {
      if (isFirstIndexOfAJoinVar(i)) {
        i += Long.BYTES - 1;
      } else {
        min[i] = Byte.MIN_VALUE;
        max[i] = Byte.MAX_VALUE;
      }
    }
    NavigableSet<byte[]> subset = multiMap.subSet(min, true, max, true);
    return new MappingIteratorWrapper(subset.iterator(), recycleCache);
  }

  private boolean isFirstIndexOfAJoinVar(int index) {
    int headerSize = Mapping.getHeaderSize();
    for (int varIndex : joinVarIndices) {
      if (index == (headerSize + (varIndex * Long.BYTES))) {
        return true;
      }
    }
    return false;
  }

  @Override
  public synchronized Iterator<Mapping> iterator() {
    return new MappingIteratorWrapper(multiMap.iterator(), recycleCache);
  }

  @Override
  public synchronized void close() {
    if (!database.isClosed()) {
      database.close();
    }
    if (mapFolder.exists()) {
      for (File file : mapFolder.listFiles()) {
        file.delete();
      }
      mapFolder.delete();
    }
  }

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
      for (int i = offset + (comparisonOrder.length * Long.BYTES); i < len; i++) {
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
      return NumberConversion.bytes2long(mapping, offset + (varIndex * Long.BYTES));
    }

    private int intCompare(int x, int y) {
      return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    private int longCompare(long x, long y) {
      return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

  }

}
