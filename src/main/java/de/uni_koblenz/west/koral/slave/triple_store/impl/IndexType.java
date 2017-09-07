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
package de.uni_koblenz.west.koral.slave.triple_store.impl;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;

/**
 * Provides methods to extract the subject, property or object of the byte array
 * stored in the the different triple indices.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public enum IndexType {

  SPO {
    @Override
    public long getSubject(byte[] triple) {
      return NumberConversion.bytes2long(triple, 0);
    }

    @Override
    public long getProperty(byte[] triple) {
      return NumberConversion.bytes2long(triple, 8);
    }

    @Override
    public long getObject(byte[] triple) {
      return NumberConversion.bytes2long(triple, 16);
    }

    @Override
    public byte[] getSPOCArray(byte[] triple) {
      return triple;
    }
  },

  OSP {
    @Override
    public long getSubject(byte[] triple) {
      return NumberConversion.bytes2long(triple, 8);
    }

    @Override
    public long getProperty(byte[] triple) {
      return NumberConversion.bytes2long(triple, 16);
    }

    @Override
    public long getObject(byte[] triple) {
      return NumberConversion.bytes2long(triple, 0);
    }

    @Override
    public byte[] getSPOCArray(byte[] triple) {
      byte[] result = new byte[triple.length];
      System.arraycopy(triple, 1 * Long.BYTES, result, 0 * Long.BYTES, Long.BYTES);
      System.arraycopy(triple, 2 * Long.BYTES, result, 1 * Long.BYTES, Long.BYTES);
      System.arraycopy(triple, 0 * Long.BYTES, result, 2 * Long.BYTES, Long.BYTES);
      if (triple.length > (3 * Long.BYTES)) {
        System.arraycopy(triple, 3 * Long.BYTES, result, 3 * Long.BYTES,
                triple.length - (3 * Long.BYTES));
      }
      return result;
    }
  },

  POS {
    @Override
    public long getSubject(byte[] triple) {
      return NumberConversion.bytes2long(triple, 16);
    }

    @Override
    public long getProperty(byte[] triple) {
      return NumberConversion.bytes2long(triple, 0);
    }

    @Override
    public long getObject(byte[] triple) {
      return NumberConversion.bytes2long(triple, 8);
    }

    @Override
    public byte[] getSPOCArray(byte[] triple) {
      byte[] result = new byte[triple.length];
      System.arraycopy(triple, 2 * Long.BYTES, result, 0 * Long.BYTES, Long.BYTES);
      System.arraycopy(triple, 0 * Long.BYTES, result, 1 * Long.BYTES, Long.BYTES);
      System.arraycopy(triple, 1 * Long.BYTES, result, 2 * Long.BYTES, Long.BYTES);
      if (triple.length > (3 * Long.BYTES)) {
        System.arraycopy(triple, 3 * Long.BYTES, result, 3 * Long.BYTES,
                triple.length - (3 * Long.BYTES));
      }
      return result;
    }
  };

  public abstract long getSubject(byte[] triple);

  public abstract long getProperty(byte[] triple);

  public abstract long getObject(byte[] triple);

  public byte[] getContainment(byte[] triple) {
    byte[] result = new byte[triple.length - (3 * Long.BYTES)];
    System.arraycopy(triple, 3 * Long.BYTES, result, 0, result.length);
    return result;
  }

  public abstract byte[] getSPOCArray(byte[] triple);

}
