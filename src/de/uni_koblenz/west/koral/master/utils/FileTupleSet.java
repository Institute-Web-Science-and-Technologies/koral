package de.uni_koblenz.west.koral.master.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Represents a set of triples that is stored as a file.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class FileTupleSet implements Iterable<String[]> {

  private final File file;

  public FileTupleSet(File file) {
    this.file = file;
  }

  public File getFile() {
    return file;
  }

  public boolean contains(String[] tuple) {
    if (!file.exists()) {
      return false;
    }
    for (String[] storedTuple : this) {
      if (Arrays.equals(tuple, storedTuple)) {
        return true;
      }
    }
    return false;
  }

  public boolean add(String[] tuple) {
    if (contains(tuple)) {
      return false;
    } else {
      append(tuple);
      return true;
    }
  }

  public void append(String[] tuple) {
    try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(
            new GZIPOutputStream(new FileOutputStream(getFile(), true))));) {
      output.writeInt(tuple.length);
      for (String element : tuple) {
        byte[] bytes = element.getBytes("UTF-8");
        output.writeInt(bytes.length);
        output.write(bytes);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterator<String[]> iterator() {
    return new Iterator<String[]>() {

      private DataInputStream input;

      private String[] next;

      {
        if (getFile().exists()) {
          try {
            input = new DataInputStream(
                    new BufferedInputStream(new GZIPInputStream(new FileInputStream(getFile()))));
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

      private String[] readNext() {
        try {
          String[] tuple = new String[input.readInt()];
          for (int i = 0; i < tuple.length; i++) {
            byte[] nextString = new byte[input.readInt()];
            input.readFully(nextString);
            tuple[i] = new String(nextString, "UTF-8");
          }
          return tuple;
        } catch (EOFException e) {
          return null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public String[] next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        String[] nextTuple = next;
        next = readNext();
        return nextTuple;
      }

    };
  }

}
