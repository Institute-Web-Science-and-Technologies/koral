package de.uni_koblenz.west.koral.slave.triple_store.impl;

import java.util.Arrays;

/**
 * Wraps an array for the use in a Collection.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class ArrayWrapper {

  private final byte[] array;

  public ArrayWrapper(byte[] array) {
    this.array = array;
  }

  public byte[] getArray() {
    return array;
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
    ArrayWrapper other = (ArrayWrapper) obj;
    if (!Arrays.equals(array, other.array)) {
      return false;
    }
    return true;
  }

}
