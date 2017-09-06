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
package de.uni_koblenz.west.koral.common.query;

import de.uni_koblenz.west.koral.common.messages.MessageType;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.slave.triple_store.impl.IndexType;

/**
 * <p>
 * In order to prevent the garbage collector to be executed frequently, this
 * class is used to create and release {@link Mapping} instances. A new
 * {@link Mapping} instance is only created if no previously released instance
 * is cached. In order to prevent the memory to be flooded by unused
 * {@link Mapping} instances, the number of cached instances is limited.
 * </p>
 * 
 * <p>
 * WARNING: This class is not thread safe in order to avoid synchronization
 * overhead!
 * </p>
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MappingRecycleCache {

  private final int numberOfSlaves;

  private final Mapping[] stack;

  private int nextFreeIndex;

  public MappingRecycleCache(int size, int numberOfSlaves) {
    this.numberOfSlaves = numberOfSlaves;
    stack = new Mapping[size];
    nextFreeIndex = 0;
  }

  private boolean isEmpty() {
    return nextFreeIndex == 0;
  }

  private boolean isFull() {
    return nextFreeIndex >= stack.length;
  }

  private void push(Mapping mapping) {
    stack[nextFreeIndex++] = mapping;
  }

  private Mapping pop() {
    Mapping mapping = stack[nextFreeIndex - 1];
    stack[nextFreeIndex - 1] = null;
    nextFreeIndex -= 1;
    return mapping;
  }

  public synchronized Mapping createMapping(TriplePattern pattern, IndexType indexType,
          byte[] triple) {
    byte[] newMapping = new byte[Byte.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES
            + (Long.BYTES * pattern.getVariables().length)
            + ((numberOfSlaves / Byte.SIZE) + ((numberOfSlaves % Byte.SIZE) == 0 ? 0 : 1))];
    newMapping[0] = MessageType.QUERY_MAPPING_BATCH.getValue();
    NumberConversion.int2bytes(newMapping.length, newMapping, Byte.BYTES + Long.BYTES + Long.BYTES);
    // set matched variables
    int insertionIndex = Byte.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES;
    if (pattern.isSubjectVariable()) {
      NumberConversion.long2bytes(indexType.getSubject(triple), newMapping, insertionIndex);
      insertionIndex += Long.BYTES;
    }
    if (pattern.isPropertyVariable()) {
      NumberConversion.long2bytes(indexType.getProperty(triple), newMapping, insertionIndex);
      insertionIndex += Long.BYTES;
    }
    if (pattern.isObjectVariable()) {
      NumberConversion.long2bytes(indexType.getObject(triple), newMapping, insertionIndex);
      insertionIndex += Long.BYTES;
    }
    System.arraycopy(triple, 3 * Long.BYTES, newMapping, insertionIndex,
            triple.length - (3 * Long.BYTES));
    return createMapping(newMapping, 0, newMapping.length);
  }

  public synchronized Mapping createMapping(byte[] byteArrayWithMapping,
          int firstIndexOfMappingInArray, int lengthOfMapping) {
    Mapping result = getMapping();
    result.set(byteArrayWithMapping, firstIndexOfMappingInArray, lengthOfMapping);
    return result;
  }

  private Mapping getMapping() {
    Mapping result;
    if (isEmpty()) {
      result = new Mapping(numberOfSlaves);
    } else {
      result = pop();
    }
    return result;
  }

  public synchronized void releaseMapping(Mapping mapping) {
    if (!isFull()) {
      push(mapping);
    }
  }

  public synchronized Mapping getMappingWithRestrictedVariables(Mapping mapping, long[] vars,
          long[] selectedVars) {
    Mapping result = getMapping();
    result.restrictMapping(selectedVars, mapping, vars);
    return result;
  }

  public synchronized Mapping mergeMappings(long[] resultVarsOrdering, Mapping mapping1,
          long[] vars1, Mapping mapping2, long[] vars2) {
    Mapping result = getMapping();
    result.joinMappings(resultVarsOrdering, mapping1, vars1, mapping2, vars2);
    return result;
  }

  public synchronized Mapping cloneMapping(Mapping mapping) {
    byte[] newArray = new byte[mapping.getLengthOfMappingInByteArray()];
    System.arraycopy(mapping.getByteArray(), mapping.getFirstIndexOfMappingInByteArray(), newArray,
            0, newArray.length);
    Mapping newMapping = createMapping(newArray, 0, newArray.length);
    return newMapping;
  }

}
