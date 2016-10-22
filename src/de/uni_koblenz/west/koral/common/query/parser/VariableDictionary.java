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
package de.uni_koblenz.west.koral.common.query.parser;

/**
 * Dictionary to decode and encode variable names.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class VariableDictionary {

  private int nextID;

  private String[] id2name;

  public VariableDictionary() {
    id2name = new String[10];
    nextID = 0;
  }

  public long encode(String varName) {
    if ((nextID & 0xff_ff_00_00) != 0) {
      throw new ArrayIndexOutOfBoundsException(
              "The maximum amount of variables has already been encoded.");
    }
    int id = -1;
    // check existence of id
    for (int i = 0; i < nextID; i++) {
      if (id2name[i].equals(varName)) {
        id = i;
        break;
      }
    }
    if (id == -1) {
      // create new id
      id = nextID++;
      if (id >= id2name.length) {
        String[] newId2name = new String[id2name.length + 10];
        System.arraycopy(id2name, 0, newId2name, 0, id2name.length);
        id2name = newId2name;
      }
      id2name[id] = varName;
    }
    return id & 0x00_00_00_00_ff_ff_ff_ffl;
  }

  public String decode(long var) {
    int index = (int) var;
    if (index >= nextID) {
      throw new IllegalArgumentException("The variable " + var + " is unknown.");
    }
    return id2name[index];
  }

  public String[] decode(long[] vars) {
    String[] result = new String[vars.length];
    for (int i = 0; i < vars.length; i++) {
      result[i] = decode(vars[i]);
    }
    return result;
  }

}
