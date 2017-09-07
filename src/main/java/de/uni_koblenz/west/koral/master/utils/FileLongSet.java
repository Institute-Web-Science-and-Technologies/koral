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

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

/**
 * Stores a sequence of long values on disk
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class FileLongSet implements Closeable, AdjacencyList {

  private final File file;

  private DataOutputStream output;

  public FileLongSet(File file) {
    this.file = file;
  }

  public File getFile() {
    return file;
  }

  public boolean contains(long value) {
    if (!file.exists()) {
      return false;
    }
    LongIterator iter = iterator();
    while (iter.hasNext()) {
      if (value == iter.next()) {
        iter.close();
        return true;
      }
    }
    iter.close();
    return false;
  }

  public boolean add(long value) {
    if (contains(value)) {
      return false;
    } else {
      append(value);
      return true;
    }
  }

  @Override
  public void append(long value) {
    try {
      if (output == null) {
        output = new DataOutputStream(new BufferedOutputStream(
                new GZIPOutputStream(new FileOutputStream(getFile(), true))));
      }
      output.writeLong(value);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public LongIterator iterator() {
    close();
    return new FileLongSetLongIterator(getFile());
  }

  @Override
  public void close() {
    if (output != null) {
      try {
        output.close();
        output = null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
