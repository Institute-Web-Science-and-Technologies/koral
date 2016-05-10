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
public class FileLongSet implements Closeable {

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

  public LongIterator iterator() {
    close();
    return new LongIterator(getFile());
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
