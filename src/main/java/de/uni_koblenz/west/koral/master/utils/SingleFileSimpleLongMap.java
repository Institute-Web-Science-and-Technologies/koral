package de.uni_koblenz.west.koral.master.utils;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

/**
 * Uses a random access file.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class SingleFileSimpleLongMap extends CachedSimpleLongMap {

  private RandomAccessFile map;

  private final boolean isZeroAllowed;

  public SingleFileSimpleLongMap(File contentFile, boolean isZeroAllowed) {
    super(contentFile);
    this.isZeroAllowed = isZeroAllowed;
    try {
      map = new RandomAccessFile(contentFile, "rw");
    } catch (IOException e) {
      close();
      throw new RuntimeException(e);
    }
  }

  @Override
  protected byte[] internalGet(byte[] keyBytes) {
    try {
      map.seek((NumberConversion.bytes2long(keyBytes) - 1) * Long.BYTES);
      byte[] result = new byte[Long.BYTES];
      map.readFully(result, 0, result.length);
      if (!isZeroAllowed && (NumberConversion.bytes2long(result) == 0)) {
        return null;
      } else {
        return result;
      }
    } catch (EOFException e) {
      return null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void writeCache(Map<Long, byte[]> cache) {
    try {
      for (Entry<Long, byte[]> entry : cache.entrySet()) {
        map.seek((entry.getKey() - 1) * Long.BYTES);
        map.write(entry.getValue(), 0, Long.BYTES);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterator<long[]> iterator() {
    flush();
    try {
      return new Iterator<long[]>() {

        private long index;
        private long next;
        {
          index = 0;
          map.seek(0);
          getNext();
        }

        @Override
        public boolean hasNext() {
          return index >= 0;
        }

        @Override
        public long[] next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          long[] n = new long[] { index, next };
          getNext();
          return n;
        }

        private void getNext() {
          try {
            do {
              next = map.readLong();
              index++;
            } while (!isZeroAllowed && (next == 0));
          } catch (IOException e) {
            index = -1;
          }
        }

      };
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    super.close();
    if (map != null) {
      try {
        map.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
