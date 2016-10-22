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

import java.io.Closeable;
import java.util.Iterator;

/**
 * A multi map.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface MultiMap extends Closeable, AutoCloseable, Iterable<byte[]> {

  public int size();

  public boolean isEmpty();

  public boolean containsKey(byte[] prefix);

  public Iterable<byte[]> get(byte[] prefix);

  public void put(byte[] content);

  public void removeAll(byte[] prefix);

  public void remove(byte[] content);

  @Override
  public Iterator<byte[]> iterator();

  public void flush();

  public void clear();

  @Override
  public void close();

}
