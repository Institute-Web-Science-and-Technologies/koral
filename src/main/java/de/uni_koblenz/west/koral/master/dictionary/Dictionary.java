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
package de.uni_koblenz.west.koral.master.dictionary;

import java.io.Closeable;

/**
 * Declares all methods required by {@link DictionaryEncoder}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface Dictionary extends Closeable {

  /**
   * if value already exists, its id is returned. Otherwise if
   * <code>createNewEncodingForUnknownNodes == true</code> a new id is generated
   * whose first two bytes are 0 and if
   * <code>createNewEncodingForUnknownNodes == false</code>, 0 is returned.
   * 
   * @param value
   * @param createNewEncodingForUnknownNodes
   * @return
   * @throws RuntimeException
   *           if maximum number of strings (i.e., 2^48) have been encoded
   */
  public long encode(String value, boolean createNewEncodingForUnknownNodes);

  /**
   * @param id
   * @return <code>null</code> if no String has been encoded to this id, yet.
   */
  public String decode(long id);

  public void flush();

  public boolean isEmpty();
  
  public long size();

  public void clear();

  @Override
  public void close();

}
