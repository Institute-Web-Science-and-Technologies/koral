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

import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedFileOutputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.Statement;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Represents a set of triples that is stored as a file.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class FileTupleSet implements Iterable<byte[][]>, Closeable {

  private final File file;

  private EncodedFileOutputStream output;

  public FileTupleSet(File file) {
    this.file = file;
  }

  public File getFile() {
    return file;
  }

  public boolean contains(byte[][] tuple) {
    if (!file.exists()) {
      return false;
    }
    for (byte[][] storedTuple : this) {
      if (tuple.length == storedTuple.length) {
        for (int i = 0; i < tuple.length; i++) {
          if (Arrays.equals(tuple[i], storedTuple[i])) {
            return true;
          }
        }
      }
    }
    return false;
  }

  public boolean add(byte[][] tuple) {
    if (contains(tuple)) {
      return false;
    } else {
      append(tuple);
      return true;
    }
  }

  public void append(byte[][] tuple) {
    try {
      if (output == null) {
        output = new EncodedFileOutputStream(getFile(), true);
      }
      Statement statement = Statement.getStatement(EncodingFileFormat.EEE, tuple[0], tuple[1],
              tuple[2], tuple[3]);
      output.writeStatement(statement);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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

  @Override
  public Iterator<byte[][]> iterator() {
    close();
    return new Iterator<byte[][]>() {

      private EncodedFileInputStream input;

      private byte[][] next;

      {
        if (getFile().exists()) {
          try {
            input = new EncodedFileInputStream(EncodingFileFormat.EEE, getFile());
            next = readNext();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }

      @Override
      public boolean hasNext() {
        if (next == null) {
          try {
            if (input != null) {
              input.close();
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return false;
        } else {
          return true;
        }
      }

      private byte[][] readNext() {
        try {
          Statement statement = input.read();
          return new byte[][] { statement.getSubject(), statement.getProperty(),
                  statement.getObject(), statement.getContainment() };
        } catch (EOFException e) {
          return null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public byte[][] next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        byte[][] nextTuple = next;
        next = readNext();
        return nextTuple;
      }

    };
  }

}
