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

import de.uni_koblenz.west.koral.common.utils.NumberConversion;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Writes long values into a file. Long values are v-byte encoded.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class EncodedRandomAccessLongFileOutputStream implements AutoCloseable, LongOutputWriter {

  private final DataOutputStream out;

  private long position;

  public EncodedRandomAccessLongFileOutputStream(File outputFile)
          throws FileNotFoundException, IOException {
    FileOutputStream fileOut = new FileOutputStream(outputFile);
    out = new DataOutputStream(new BufferedOutputStream(fileOut));
    position = 0;
  }

  public long getPosition() {
    return position;
  }

  @Override
  public void writeLong(long value) throws IOException {
    write(NumberConversion.long2bytes(value));
  }

  public void write(byte[] element) throws IOException {
    // v-byte encoding
    int numberOfBits = element.length * Byte.SIZE;
    int numberOfBlocks = (numberOfBits / 7) + ((numberOfBits % 7) == 0 ? 0 : 1);
    boolean isFirstBlockWritten = false;
    for (int blockNumber = 0; blockNumber < numberOfBlocks; blockNumber++) {
      int nextBlock = get7BitBlock(element, blockNumber);
      if (isFirstBlockWritten
              || (nextBlock != 0) /* the last block is always <0 */) {
        out.writeByte(nextBlock);
        position++;
        isFirstBlockWritten = true;
      }
    }
  }

  private int get7BitBlock(byte[] element, int blockIndexNumber) {
    int numberOfBits = element.length * Byte.SIZE;
    int numberOfBlocks = (numberOfBits / 7) + ((numberOfBits % 7) == 0 ? 0 : 1);
    int firstBlockSize = numberOfBits % 7;
    if (firstBlockSize == 0) {
      firstBlockSize = 7;
    }
    int result = 0;
    if (blockIndexNumber == 0) {
      // this is the first block
      // since all 7 bit blocks are filled from right to left, the first block
      // only contains the remaining bits
      result = element[0] & 0xff_ff;
      switch (firstBlockSize) {
        case 1:
          result = (result & 0b1000_0000) >>> 7;
          break;
        case 2:
          result = (result & 0b1100_0000) >>> 6;
          break;
        case 3:
          result = (result & 0b1110_0000) >>> 5;
          break;
        case 4:
          result = (result & 0b1111_0000) >>> 4;
          break;
        case 5:
          result = (result & 0b1111_1000) >>> 3;
          break;
        case 6:
          result = (result & 0b1111_1100) >>> 2;
          break;
        case 7:
          result = (result & 0b1111_1110) >>> 1;
          break;
        default:
          // this is not possible
      }
    } else {
      int firstBitIndex = firstBlockSize + ((blockIndexNumber - 1) * 7);
      int firstByteIndex = firstBitIndex / Byte.SIZE;
      int firstBitIndexInByte = firstBitIndex % Byte.SIZE;
      result = element[firstByteIndex];
      switch (firstBitIndexInByte) {
        case 0:
          result = (result & 0b1111_1110) >>> 1;
          break;
        case 1:
          result = result & 0b0111_1111;
          break;
        case 2:
          result = (result & 0b0011_1111) << 1;
          break;
        case 3:
          result = (result & 0b0001_1111) << 2;
          break;
        case 4:
          result = (result & 0b0000_1111) << 3;
          break;
        case 5:
          result = (result & 0b0000_0111) << 4;
          break;
        case 6:
          result = (result & 0b0000_0011) << 5;
          break;
        case 7:
          result = (result & 0b0000_0001) << 6;
          break;
        default:
          // this is not possible
      }
      if (firstBitIndexInByte > 1) {
        // the 7 bit block is incomplete
        // read rest from next byte
        assert (firstByteIndex
                + 1) < numberOfBlocks : "The last byte should be contained completely in the last block.";
        int rest = element[firstByteIndex + 1];
        switch (firstBitIndexInByte) {
          case 2:
            rest = (rest & 0b1000_0000) >>> 7;
            break;
          case 3:
            rest = (rest & 0b1100_0000) >>> 6;
            break;
          case 4:
            rest = (rest & 0b1110_0000) >>> 5;
            break;
          case 5:
            rest = (rest & 0b1111_0000) >>> 4;
            break;
          case 6:
            rest = (rest & 0b1111_1000) >>> 3;
            break;
          case 7:
            rest = (rest & 0b1111_1100) >>> 2;
            break;
          default:
            // this is not possible
        }
        result |= rest;
      }
    }
    if (blockIndexNumber == (numberOfBlocks - 1)) {
      // this is the last bit
      result = result | 0b1000_0000;
    }
    return result;
  }

  @Override
  public void close() throws IOException {
    if (out != null) {
      out.close();
    }
  }

}
