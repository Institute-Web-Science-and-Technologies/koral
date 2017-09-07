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

import de.uni_koblenz.west.koral.common.executor.WorkerTask;
import de.uni_koblenz.west.koral.common.executor.messagePassing.MessageSenderBuffer;
import de.uni_koblenz.west.koral.common.messages.MessageType;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;

/**
 * <p>
 * This class represents one mapping produced by a query operation. The mapping
 * is stored as a byte array. If a mapping batch is received, the batch, i.e., a
 * byte array, is shared among several instances of this class.
 * </p>
 * 
 * <p>
 * To avoid garbage collection, instances of this class are reused. Therefore,
 * use {@link MappingRecycleCache} to create instances of this class and to
 * release them again.
 * </p>
 * 
 * <p>
 * Thus, instances of this class should be seen as immutable from within a
 * {@link WorkerTask}. Additionally, pointers to an instance should be removed
 * when it is sent to another task or any other operation is performed that
 * might lead to a {@link MappingRecycleCache#releaseMapping(Mapping)} call.
 * </p>
 * 
 * @see MessageSenderBuffer
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class Mapping {

  private final int numberOfSlaves;

  /**
   * This array consists of:
   * <ol>
   * <li>1 byte message type</li>
   * <li>8 byte receiving query task id</li>
   * <li>8 byte sender task id</li>
   * <li>4 byte length of mapping serialization</li>
   * <li>mapping serialization
   * <ol>
   * <li>8*#vars bytes of mapping</li>
   * <li>{@link #numberOfSlaves}/8+({@link #numberOfSlaves}%8==0?0:1) bytes of
   * containment serialization</li>
   * </ol>
   * </li>
   * </ol>
   */
  private byte[] byteArray;

  private int firstIndex;

  private int length;

  Mapping(int numberOfSlaves) {
    this.numberOfSlaves = numberOfSlaves;
  }

  void set(byte[] byteArrayWithMapping, int firstIndexOfMappingInArray, int lengthOfMapping) {
    byteArray = byteArrayWithMapping;
    firstIndex = firstIndexOfMappingInArray;
    length = lengthOfMapping;
  }

  private void set(byte[] newMapping) {
    byteArray = newMapping;
    firstIndex = 0;
    length = newMapping.length;
  }

  public int getNumberOfContainmentBytes() {
    return (numberOfSlaves / Byte.SIZE) + ((numberOfSlaves % Byte.SIZE) == 0 ? 0 : 1);
  }

  public static int getHeaderSize() {
    return Byte.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES;
  }

  private int getLengthOfMapping(int numberOfVars) {
    return Mapping.getHeaderSize() + (numberOfVars * Long.BYTES) + getNumberOfContainmentBytes();
  }

  public void updateReceiver(long receiverTaskID) {
    NumberConversion.long2bytes(receiverTaskID, byteArray, firstIndex + Byte.BYTES);
  }

  public void updateSender(long senderTaskID) {
    NumberConversion.long2bytes(senderTaskID, byteArray, firstIndex + Byte.BYTES + Long.BYTES);
  }

  public void setContainmentToAll() {
    int sizeOfContainment = getNumberOfContainmentBytes();
    if ((numberOfSlaves % Byte.SIZE) == 0) {
      for (int i = 0; i < sizeOfContainment; i++) {
        byteArray[((firstIndex + length) - sizeOfContainment) + i] = (byte) 0xff;
      }
    } else {
      for (int i = 0; i < (sizeOfContainment - 1); i++) {
        byteArray[((firstIndex + length) - sizeOfContainment) + i] = (byte) 0xff;
      }
      int remainingComputers = numberOfSlaves % Byte.SIZE;
      switch (remainingComputers) {
        case 1:
          byteArray[(firstIndex + length) - 1] = (byte) 0x80;
          break;
        case 2:
          byteArray[(firstIndex + length) - 1] = (byte) 0xc0;
          break;
        case 3:
          byteArray[(firstIndex + length) - 1] = (byte) 0xe0;
          break;
        case 4:
          byteArray[(firstIndex + length) - 1] = (byte) 0xf0;
          break;
        case 5:
          byteArray[(firstIndex + length) - 1] = (byte) 0xf8;
          break;
        case 6:
          byteArray[(firstIndex + length) - 1] = (byte) 0xfc;
          break;
        case 7:
          byteArray[(firstIndex + length) - 1] = (byte) 0xfe;
          break;
      }
    }
  }

  public void updateContainment(int currentContainingComputer, int nextContainingComputer) {
    if (getNumberOfContainmentBytes() == 0) {
      return;
    }
    // remove old containing computer
    int byteIndex = getContainingByte(currentContainingComputer);
    byte bitMask = getBitMaskFor(currentContainingComputer);
    byteArray[byteIndex] = (byte) (byteArray[byteIndex] & ~bitMask);
    // set new containingComputer
    byteIndex = getContainingByte(nextContainingComputer);
    bitMask = getBitMaskFor(nextContainingComputer);
    byteArray[byteIndex] = (byte) (byteArray[byteIndex] | bitMask);
  }

  private int getContainingByte(int computerId) {
    computerId -= 1;
    return ((firstIndex + length) - getNumberOfContainmentBytes()) + (computerId / Byte.SIZE);
  }

  private byte getBitMaskFor(int computerId) {
    computerId -= 1;
    switch (computerId % Byte.SIZE) {
      case 0:
        return (byte) 0x80;
      case 1:
        return (byte) 0x40;
      case 2:
        return (byte) 0x20;
      case 3:
        return (byte) 0x10;
      case 4:
        return (byte) 0x08;
      case 5:
        return (byte) 0x04;
      case 6:
        return (byte) 0x02;
      case 7:
        return (byte) 0x01;
    }
    return 0;
  }

  public void restrictMapping(long[] resultVars, Mapping mapping, long[] vars) {
    byte[] newMapping = createNewMappingArray(resultVars.length);
    for (int i = 0; i < resultVars.length; i++) {
      NumberConversion.long2bytes(mapping.getValue(resultVars[i], vars), newMapping,
              Mapping.getHeaderSize() + (i * Long.BYTES));
    }
    if (getNumberOfContainmentBytes() > 0) {
      System.arraycopy(mapping.getByteArray(),
              (mapping.getFirstIndexOfMappingInByteArray()
                      + mapping.getLengthOfMappingInByteArray()) - getNumberOfContainmentBytes(),
              newMapping, newMapping.length - getNumberOfContainmentBytes(),
              getNumberOfContainmentBytes());
    }
    set(newMapping);
  }

  public void joinMappings(long[] resultVarsOrdering, Mapping mapping1, long[] vars1,
          Mapping mapping2, long[] vars2) {
    if (mapping2.isEmptyMapping()) {
      byteArray = mapping1.getByteArray();
      firstIndex = mapping1.getFirstIndexOfMappingInByteArray();
      length = mapping1.getLengthOfMappingInByteArray();
    } else if (mapping1.isEmptyMapping()) {
      byteArray = mapping2.getByteArray();
      firstIndex = mapping2.getFirstIndexOfMappingInByteArray();
      length = mapping2.getLengthOfMappingInByteArray();
    } else {
      byte[] newMapping = createNewMappingArray(resultVarsOrdering.length);
      int nextFreeIndex = Mapping.getHeaderSize();
      for (int nextVarIndex = 0; nextVarIndex < resultVarsOrdering.length; nextVarIndex++) {
        long valueOfVar = 0;
        try {
          valueOfVar = mapping1.getValue(resultVarsOrdering[nextVarIndex], vars1);
        } catch (IllegalArgumentException e) {
          // mapping1 does not contain the variable
          valueOfVar = mapping2.getValue(resultVarsOrdering[nextVarIndex], vars2);
        }
        NumberConversion.long2bytes(valueOfVar, newMapping, nextFreeIndex);
        nextFreeIndex += Long.BYTES;
      }
      set(newMapping);
    }
    // intersect containment
    for (int i = 0; i < getNumberOfContainmentBytes(); i++) {
      byteArray[byteArray.length - 1
              - i] = (byte) (mapping1
                      .getByteArray()[(mapping1.getFirstIndexOfMappingInByteArray()
                              + mapping1.getLengthOfMappingInByteArray()) - 1 - i]
                      & mapping2.getByteArray()[(mapping2.getFirstIndexOfMappingInByteArray()
                              + mapping2.getLengthOfMappingInByteArray()) - 1 - i]);
    }
  }

  private byte[] createNewMappingArray(int numberOfVars) {
    byte[] newMapping = new byte[getLengthOfMapping(numberOfVars)];
    newMapping[0] = MessageType.QUERY_MAPPING_BATCH.getValue();
    NumberConversion.int2bytes(newMapping.length, newMapping, Byte.BYTES + Long.BYTES + Long.BYTES);
    return newMapping;
  }

  public byte[] getByteArray() {
    return byteArray;
  }

  /**
   * @return index which represents the message type of this {@link Mapping}
   */
  public int getFirstIndexOfMappingInByteArray() {
    return firstIndex;
  }

  public int getLengthOfMappingInByteArray() {
    return length;
  }

  /**
   * if var = -1 the value of the first variable is returned
   * 
   * @param var
   * @param vars
   * @return
   * @throws IllegalArgumentException
   *           if this is an empty mapping or var is not contained in vars.
   */
  public long getValue(long var, long[] vars) {
    if (isEmptyMapping()) {
      throw new IllegalArgumentException("An empty mapping does not have any values.");
    }
    int indexOfVar = 0;
    if (var >= 0) {
      for (; indexOfVar < vars.length; indexOfVar++) {
        if (var == vars[indexOfVar]) {
          break;
        }
      }
    }
    if (indexOfVar >= vars.length) {
      throw new IllegalArgumentException("The variable " + var + " is not bound in this mapping.");
    }
    int indexOfMapping = getFirstIndexOfMappingInByteArray() + Mapping.getHeaderSize()
            + (indexOfVar * Long.BYTES);
    return NumberConversion.bytes2long(getByteArray(), indexOfMapping);
  }

  public boolean isEmptyMapping() {
    return length == getLengthOfMapping(0);
  }

  public short getIdOfFirstComputerKnowingThisMapping() {
    for (int i = length - getNumberOfContainmentBytes(); i < length; i++) {
      int numberOfAlreadyReadBytes = i - (length - getNumberOfContainmentBytes());
      if (byteArray[i] != 0) {
        int value = (byteArray[i] & 0x00_00_00_ff) << (Integer.SIZE - Byte.SIZE);
        for (int numberOfReadBits = 0; numberOfReadBits < Byte.SIZE; numberOfReadBits++) {
          if (value < 0) {
            return (short) (((numberOfAlreadyReadBytes * Byte.SIZE) + numberOfReadBits) + 1);
          }
          value <<= 1;
        }
      }
    }
    return -1;
  }

  public boolean isKnownByComputer(int computerId) {
    int byteIndex = getContainingByte(computerId);
    byte bitMask = getBitMaskFor(computerId);
    return ((byte) (byteArray[byteIndex] & bitMask)) != 0;
  }

  public String toString(long[] vars) {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getName()).append("[");
    sb.append("type").append("=")
            .append(MessageType.valueOf(byteArray[getFirstIndexOfMappingInByteArray()]));
    sb.append(", ");
    sb.append("receiver").append("=")
            .append(getTaskIdString(getFirstIndexOfMappingInByteArray() + Byte.BYTES));
    sb.append(", ");
    sb.append("sender").append("=")
            .append(getTaskIdString(getFirstIndexOfMappingInByteArray() + Byte.BYTES + Long.BYTES));
    sb.append(", ");
    sb.append("length").append("=").append(NumberConversion.bytes2int(byteArray,
            getFirstIndexOfMappingInByteArray() + Byte.BYTES + Long.BYTES + Long.BYTES));
    sb.append(", ");
    sb.append("mappings").append("=").append("{");
    for (int i = 0; i < vars.length; i++) {
      sb.append(i == 0 ? "" : ",").append(vars[i]).append("->").append(NumberConversion.bytes2long(
              byteArray,
              getFirstIndexOfMappingInByteArray() + Mapping.getHeaderSize() + (i * Long.BYTES)));
    }
    sb.append("}");
    sb.append(", ");
    sb.append("containingComputers").append("=").append(printContainment());
    sb.append("]");
    return sb.toString();
  }

  private String getTaskIdString(int startIndex) {
    return NumberConversion.bytes2long(byteArray, startIndex) + " (computer="
            + NumberConversion.bytes2short(byteArray, startIndex) + ",query="
            + NumberConversion.bytes2int(byteArray, startIndex + Short.BYTES) + ",task="
            + NumberConversion.bytes2short(byteArray, startIndex + Short.BYTES + Integer.BYTES)
            + ")";
  }

  public String printContainment() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    String delim = "";
    for (int i = 0; i < (getNumberOfContainmentBytes() * Byte.SIZE); i++) {
      if (isKnownByComputer(i + 1)) {
        sb.append(delim).append(i + 1);
        delim = ",";
      }
    }
    sb.append("}");
    return sb.toString();
  }

}
