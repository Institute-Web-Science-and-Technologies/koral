package de.uni_koblenz.west.koral.master.utils;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

/**
 * Provides a {@link SimpleLongMap} with a caching layer.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public abstract class CachedSimpleLongMap implements SimpleLongMap {

  private static final int MAX_CACHE_SIZE = 65536;

  private final File contentFile;

  private final Map<Long, byte[]> cache;

  public CachedSimpleLongMap(File contentFile) {
    this.contentFile = contentFile;
    cache = new TreeMap<>();
  }

  @Override
  public void put(long key, long value) {
    if (cache.size() == CachedSimpleLongMap.MAX_CACHE_SIZE) {
      flush();
    }
    cache.put(key, NumberConversion.long2bytes(value));
  }

  @Override
  public long get(long key) throws NoSuchElementException {
    byte[] value = cache.get(key);
    if (value == null) {
      byte[] keyBytes = NumberConversion.long2bytes(key);
      value = internalGet(keyBytes);
    }
    if (value == null) {
      throw new NoSuchElementException();
    } else {
      return NumberConversion.bytes2long(value);
    }
  }

  protected abstract byte[] internalGet(byte[] keyBytes);

  @Override
  public void flush() {
    writeCache(cache);
    cache.clear();
  }

  protected abstract void writeCache(Map<Long, byte[]> cache);

  @Override
  public void close() {
    cache.clear();
    deleteFolder(contentFile);
  }

  private void deleteFolder(File folder) {
    if (!folder.exists()) {
      return;
    }
    if (folder.isDirectory()) {
      Path path = FileSystems.getDefault().getPath(folder.getAbsolutePath());
      try {
        Files.walkFileTree(path, new FileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                  throws IOException {
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                  throws IOException {
            // here you have the files to process
            file.toFile().delete();
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            return FileVisitResult.TERMINATE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
          }
        });
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    folder.delete();
  }

}
