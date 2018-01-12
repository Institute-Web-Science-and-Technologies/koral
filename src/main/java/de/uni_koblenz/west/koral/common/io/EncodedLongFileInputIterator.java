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

import de.uni_koblenz.west.koral.master.utils.LongIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Iterator over an input file
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class EncodedLongFileInputIterator implements LongIterator {

  private final EncodedLongFileInputStream input;

  private long next;

  private boolean hasNext;

  public EncodedLongFileInputIterator(EncodedLongFileInputStream input) {
    this.input = input;
    hasNext = true;
    getNext();
  }

  private void getNext() {
    try {
      next = input.readLong();
    } catch (EOFException e) {
      hasNext = false;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean hasNext() {
    return hasNext;
  }

  @Override
  public long next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    long next = this.next;
    getNext();
    return next;
  }

  @Override
  public void close() {
  }

}
