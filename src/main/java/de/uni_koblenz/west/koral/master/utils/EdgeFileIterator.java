package de.uni_koblenz.west.koral.master.utils;

import de.uni_koblenz.west.koral.common.io.EncodedLongFileInputStream;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

/**
 * 
 * Iterates over all edges stored in a file.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class EdgeFileIterator extends EdgeIterator {

  private EncodedLongFileInputStream input;

  public EdgeFileIterator(File edgeColors) {
    try {
      input = new EncodedLongFileInputStream(edgeColors);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    next = getNext();
  }

  @Override
  protected long[] getNext() {
    if (input == null) {
      // the input is closed
      return null;
    }
    next = new long[3];
    try {
      next[0] = input.readLong();
      next[1] = input.readLong();
      next[2] = input.readLong();
    } catch (EOFException e) {
      next = null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return next;
  }

  @Override
  public void close() {
    if (input != null) {
      try {
        input.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      input = null;
    }
  }

}
