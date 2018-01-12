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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

/**
 * Reads long values from a file. The v-byte encoded long values in the input
 * file are decoded.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class EncodedLongFileInputStream implements AutoCloseable {

  private final DataInputStream input;

  private final File inputFile;

  /**
   * The input must be closed!
   * 
   * @param input
   * @throws FileNotFoundException
   * @throws IOException
   */
  public EncodedLongFileInputStream(EncodedLongFileInputStream input)
          throws FileNotFoundException, IOException {
    this(input.inputFile);
  }

  public EncodedLongFileInputStream(File inputFile) throws FileNotFoundException, IOException {
    super();
    this.inputFile = inputFile;
    input = new DataInputStream(
            new BufferedInputStream(new GZIPInputStream(new FileInputStream(inputFile))));
  }

  public long readLong() throws EOFException, IOException {
    long result = 0;
    byte currentBlock;
    do {
      currentBlock = input.readByte();
      result = result << 7;
      long value = currentBlock & 0b0111_1111;
      result = result | value;
    } while (currentBlock >= 0);
    return result;
  }

  public LongIterator iterator() {
    return new EncodedLongFileInputIterator(this);
  }

  @Override
  public void close() throws IOException {
    if (input != null) {
      input.close();
    }
  }

}
