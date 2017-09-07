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

import de.uni_koblenz.west.koral.common.query.Mapping;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This is a naive in-memory {@link JoinMappingCache} implementation which uses
 * a {@link HashMap} that maps the occurring join variable values to all
 * compatible mappings.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class InMemoryJoinMappingCache implements JoinMappingCache {

  private final Map<ByteArrayWrapper, Collection<Mapping>> mappings;

  private final long[] variables;

  private final int[] joinVarIndices;

  private long size;

  public InMemoryJoinMappingCache(long[] mappingVariables, int[] variableComparisonOrder,
          int numberOfJoinVars) {
    variables = mappingVariables;
    joinVarIndices = new int[numberOfJoinVars];
    for (int i = 0; i < numberOfJoinVars; i++) {
      joinVarIndices[i] = variableComparisonOrder[i];
    }
    size = 0;
    mappings = new ConcurrentHashMap<>();
  }

  @Override
  public Iterator<Mapping> iterator() {
    return new Iterator<Mapping>() {

      private final Iterator<Collection<Mapping>> valueIterator = mappings.values().iterator();

      private Iterator<Mapping> currentMappingIterator;

      private Mapping next = getNextMapping();

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public Mapping next() {
        if (next == null) {
          throw new NoSuchElementException();
        }
        Mapping result = next;
        next = getNextMapping();
        return result;
      }

      private Mapping getNextMapping() {
        while ((currentMappingIterator == null) || !currentMappingIterator.hasNext()) {
          if (valueIterator.hasNext()) {
            currentMappingIterator = valueIterator.next().iterator();
          } else {
            return null;
          }
        }
        return currentMappingIterator.next();
      }

    };
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public void add(Mapping mapping) {
    size++;
    ByteArrayWrapper mappingKey = getJoinVars(mapping, variables);
    Collection<Mapping> mappingSet = mappings.get(mappingKey);
    if (mappingSet == null) {
      mappingSet = new ConcurrentLinkedQueue<>();
      mappings.put(mappingKey, mappingSet);
    }
    mappingSet.add(mapping);
  }

  private ByteArrayWrapper getJoinVars(Mapping mapping, long[] mappingVars) {
    byte[] joinVars = new byte[joinVarIndices.length * Long.BYTES];
    for (int i = 0; i < joinVarIndices.length; i++) {
      int indexOfJoinVar;
      for (indexOfJoinVar = 0; indexOfJoinVar < mappingVars.length; indexOfJoinVar++) {
        if (variables[joinVarIndices[i]] == mappingVars[indexOfJoinVar]) {
          break;
        }
      }
      if (indexOfJoinVar > mappingVars.length) {
        throw new ArrayIndexOutOfBoundsException("The variable " + variables[joinVarIndices[i]]
                + " could not be found in the variables of the mapping "
                + Arrays.toString(mappingVars) + ".");
      }
      NumberConversion.long2bytes(mapping.getValue(mappingVars[indexOfJoinVar], mappingVars),
              joinVars, i * Long.BYTES);
    }
    return new ByteArrayWrapper(joinVars);
  }

  @Override
  public Iterator<Mapping> getMatchCandidates(Mapping mapping, long[] mappingVars) {
    ByteArrayWrapper mappingKey = getJoinVars(mapping, mappingVars);
    Collection<Mapping> mappingSet = mappings.get(mappingKey);
    if (mappingSet != null) {
      return mappingSet.iterator();
    } else {
      return new Iterator<Mapping>() {

        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public Mapping next() {
          throw new NoSuchElementException();
        }

      };
    }
  }

  @Override
  public void close() {
    Iterator<Entry<ByteArrayWrapper, Collection<Mapping>>> iter = mappings.entrySet().iterator();
    while (iter.hasNext()) {
      iter.next();
      iter.remove();
    }
  }

  private static class ByteArrayWrapper {

    private final byte[] array;

    public ByteArrayWrapper(byte[] wrappedArray) {
      array = wrappedArray;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + Arrays.hashCode(array);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      ByteArrayWrapper other = (ByteArrayWrapper) obj;
      if (!Arrays.equals(array, other.array)) {
        return false;
      }
      return true;
    }

  }

}
