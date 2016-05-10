package de.uni_koblenz.west.koral.master.utils;

import java.io.BufferedInputStream;
import java.io.Closeable;
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
public class LongIterator implements Closeable {

  private DataInputStream input;

  private long next;

  private boolean hasNext;

  public LongIterator(File file) {
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
