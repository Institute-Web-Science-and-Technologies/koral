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
package de.uni_koblenz.west.koral.common.query.execution;

public enum QueryOperatorType {

  PROJECTION, TRIPLE_PATTERN_JOIN, TRIPLE_PATTERN_MATCH;

  public static QueryOperatorType valueOf(int operatorType) {
    QueryOperatorType[] operatorTypes = QueryOperatorType.values();
    if (operatorType < operatorTypes.length) {
      return operatorTypes[operatorType];
    }
    throw new IllegalArgumentException(
            "There does not exist a query operator of type " + operatorType + ".");
  }

}
