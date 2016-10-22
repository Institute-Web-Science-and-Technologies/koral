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
package de.uni_koblenz.west.koral.common.io;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/**
 * A singleton class that provides method to get triple elements as String or
 * long
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class Statement {

  private static Statement singleton;

  private EncodingFileFormat format;

  private byte[] subject;

  private byte[] property;

  private byte[] object;

  private byte[] containment;

  private Statement() {
  }

  public EncodingFileFormat getFormat() {
    return format;
  }

  public byte[] getSubject() {
    return subject;
  }

  public boolean isSubjectEncoded() {
    return format.isSubjectEncoded();
  }

  public String getSubjectAsString() {
    if (format.isSubjectEncoded()) {
      throw new RuntimeException("Subject is a long and not a String. Call getSubjectAsLong()");
    }
    return convertToString(subject);
  }

  public long getSubjectAsLong() {
    if (!format.isSubjectEncoded()) {
      throw new RuntimeException("Subject is a String and not a long. Call getSubjectAsString()");
    }
    return NumberConversion.bytes2long(subject);
  }

  public byte[] getProperty() {
    return property;
  }

  public boolean isPropertyEncoded() {
    return format.isPropertyEncoded();
  }

  public String getPropertyAsString() {
    if (format.isPropertyEncoded()) {
      throw new RuntimeException("Property is a long and not a String. Call getPropertyAsLong()");
    }
    return convertToString(property);
  }

  public long getPropertyAsLong() {
    if (!format.isPropertyEncoded()) {
      throw new RuntimeException("Property is a String and not a long. Call getPropertyAsString()");
    }
    return NumberConversion.bytes2long(property);
  }

  public byte[] getObject() {
    return object;
  }

  public boolean isObjectEncoded() {
    return format.isObjectEncoded();
  }

  public String getObjectAsString() {
    if (format.isObjectEncoded()) {
      throw new RuntimeException("Object is a long and not a String. Call getObjectAsLong()");
    }
    return convertToString(object);
  }

  public long getObjectAsLong() {
    if (!format.isObjectEncoded()) {
      throw new RuntimeException("Object is a String and not a long. Call getObjectAsString()");
    }
    return NumberConversion.bytes2long(object);
  }

  public byte[] getContainment() {
    return containment;
  }

  private String convertToString(byte[] element) {
    try {
      return new String(element, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return "Statement(" + (isSubjectEncoded() ? getSubjectAsLong() : getSubjectAsString()) + ", "
            + (isPropertyEncoded() ? getPropertyAsLong() : getPropertyAsString()) + ", "
            + (isObjectEncoded() ? getObjectAsLong() : getObjectAsString()) + ", "
            + Arrays.toString(getContainment()) + ")";
  }

  public static Statement getStatement(EncodingFileFormat format, byte[] subject, byte[] property,
          byte[] object, byte[] containment) {
    if (Statement.singleton == null) {
      Statement.singleton = new Statement();
    }
    Statement.singleton.format = format;
    Statement.singleton.subject = subject;
    Statement.singleton.property = property;
    Statement.singleton.object = object;
    Statement.singleton.containment = containment;
    return Statement.singleton;
  }
}
