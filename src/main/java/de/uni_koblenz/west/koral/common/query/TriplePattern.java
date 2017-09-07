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

/**
 * Represents a triple pattern in SPARQL.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class TriplePattern {

  private final TriplePatternType type;

  private final long subject;

  private final long property;

  private final long object;

  public TriplePattern(TriplePatternType type, long subject, long property, long object) {
    this.type = type;
    this.subject = subject;
    this.property = property;
    this.object = object;
  }

  public TriplePatternType getType() {
    return type;
  }

  public boolean isSubjectVariable() {
    return (type == TriplePatternType.___) || (type == TriplePatternType.__O)
            || (type == TriplePatternType._P_) || (type == TriplePatternType._PO);
  }

  public long getSubject() {
    return subject;
  }

  public boolean isPropertyVariable() {
    return (type == TriplePatternType.___) || (type == TriplePatternType.S__)
            || (type == TriplePatternType.__O) || (type == TriplePatternType.S_O);
  }

  public long getProperty() {
    return property;
  }

  public boolean isObjectVariable() {
    return (type == TriplePatternType.___) || (type == TriplePatternType.S__)
            || (type == TriplePatternType._P_) || (type == TriplePatternType.SP_);
  }

  public long getObject() {
    return object;
  }

  public long[] getVariables() {
    switch (type) {
      case ___:
        return new long[] { subject, property, object };
      case S__:
        return new long[] { property, object };
      case _P_:
        return new long[] { subject, object };
      case __O:
        return new long[] { subject, property };
      case SP_:
        return new long[] { object };
      case S_O:
        return new long[] { property };
      case _PO:
        return new long[] { subject };
      case SPO:
      default:
        return new long[] {};
    }
  }

}
