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

/**
 * Defines which elements of a triple are already encoded. U stands for
 * unencoded, E stands for encoded.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public enum EncodingFileFormat {

  UUU, EUU, UEU, UUE, EEU, EUE, UEE, EEE;

  public boolean isSubjectEncoded() {
    return (this == EUU) || (this == EEU) || (this == EUE) || (this == EEE);
  }

  public boolean isPropertyEncoded() {
    return (this == UEU) || (this == EEU) || (this == UEE) || (this == EEE);
  }

  public boolean isObjectEncoded() {
    return (this == UUE) || (this == EUE) || (this == UEE) || (this == EEE);
  }

}
