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

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

/**
 * This class defines the different type of triple pattern known by SPARQL.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public enum TriplePatternType {

  ___, S__, _P_, __O, SP_, S_O, _PO, SPO;

  public static TriplePatternType valueOf(int value) {
    for (TriplePatternType type : TriplePatternType.values()) {
      if (type.ordinal() == value) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown " + TriplePattern.class.getName()
            + " constant with octal value " + value + ".");
  }

  public static TriplePatternType valueOf(Triple triple) {
    Node subject = triple.getSubject();
    Node property = triple.getPredicate();
    Node object = triple.getObject();
    String pattern = (subject.isVariable() ? "_" : "S") + (property.isVariable() ? "_" : "P")
            + (object.isVariable() ? "_" : "O");
    return TriplePatternType.valueOf(pattern);
  }

}
