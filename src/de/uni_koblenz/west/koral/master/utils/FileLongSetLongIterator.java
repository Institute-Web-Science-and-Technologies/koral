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
package de.uni_koblenz.west.koral.master.utils;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;

/**
 * An iterator over long values read from a file created by {@link FileLongSet}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class FileLongSetLongIterator implements LongIterator {

  private DataInputStream input;

  private long next;

  private boolean hasNext;

  public FileLongSetLongIterator(File file) {
    if (file.exists()) {
      try {
        input = new DataInputStream(
                new BufferedInputStream(new GZIPInputStream(new FileInputStream(file))));
        next = readNext();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      hasNext = false;
    }
  }

  @Override
  public boolean hasNext() {
    if (!hasNext) {
      close();
      return false;
    } else {
      return true;
    }
  }

  private long readNext() {
    try {
      long next = input.readLong();
      hasNext = true;
      return next;
    } catch (EOFException e) {
      hasNext = false;
      return 0;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    long nextValue = next;
    next = readNext();
    return nextValue;
  }

  @Override
  public void close() {
    try {
      if (input != null) {
        input.close();
        input = null;
        hasNext = false;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
