package de.uni_koblenz.west.cidre.common.utils;

import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;

import java.io.Closeable;
import java.io.File;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * <p>
 * Hash set that stores mappings in memory. If a limit is reached, mappings are
 * stored on disk.
 * </p>
 * 
 * <p>
 * Buggy and slow implementation
 * </p>
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class UnlimitedMappingHashSet implements Closeable, Iterator<Mapping>, Iterable<Mapping> {

  private final UnlimitedMappingCache[] buckets;

  private final int mappingLimitPerCache;

  private final File cacheDirectory;

  private final MappingRecycleCache recycleCache;

  private final String uniqueFileNameSuffix;

  private long size;

  private Iterator<Mapping> next;

  private int nextIteratedBucket;

  public UnlimitedMappingHashSet(int inMemoryMappingLimit, int numberOfHashBuckets,
          File cacheDirectory, MappingRecycleCache recycleCache, String uniqueFileNameSuffix) {
    super();
    buckets = new UnlimitedMappingCache[numberOfHashBuckets];
    int number = inMemoryMappingLimit / numberOfHashBuckets;
    if (number <= 0) {
      number = 1;
    }
    mappingLimitPerCache = number;
    this.cacheDirectory = cacheDirectory;
    this.recycleCache = recycleCache;
    this.uniqueFileNameSuffix = uniqueFileNameSuffix;
    size = 0;
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public long size() {
    return size;
  }

  private int getBucketIndex(Mapping mapping, long joinVar, long[] vars) {
    return mapping.isEmptyMapping() ? 0 : ((int) mapping.getValue(joinVar, vars)) % buckets.length;
  }

  public void add(Mapping mapping, long joinVar, long[] vars) {
    size++;
    int bucketIndex = getBucketIndex(mapping, joinVar, vars);
    if (buckets[bucketIndex] == null) {
      buckets[bucketIndex] = new UnlimitedMappingCache(mappingLimitPerCache, cacheDirectory,
              recycleCache, uniqueFileNameSuffix + "_bucket" + bucketIndex);
    }
    buckets[bucketIndex].append(mapping);
  }

  public Iterator<Mapping> getMatchCandidates(Mapping mapping, long joinVar, long[] vars) {
    int bucketIndex = getBucketIndex(mapping, joinVar, vars);
    if (buckets[bucketIndex] != null) {
      return buckets[bucketIndex].iterator();
    } else {
      return new Iterator<Mapping>() {

        @Override
        public Mapping next() {
          throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
          return false;
        }
      };
    }
  }

  @Override
  public Iterator<Mapping> iterator() {
    nextIteratedBucket = 0;
    next = getNext();
    if (next != null) {
      return this;
    } else {
      return new Iterator<Mapping>() {

        @Override
        public Mapping next() {
          throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
          return false;
        }
      };
    }
  }

  @Override
  public boolean hasNext() {
    return next != null && next.hasNext();
  }

  @Override
  public Mapping next() {
    if (next == null) {
      throw new NoSuchElementException();
    } else {
      Mapping n = next.next();
      if (!next.hasNext()) {
        next = getNext();
      }
      return n;
    }
  }

  private Iterator<Mapping> getNext() {
    Iterator<Mapping> next = null;
    for (; nextIteratedBucket < buckets.length; nextIteratedBucket++) {
      if (buckets[nextIteratedBucket] == null) {
        continue;
      } else {
        next = buckets[nextIteratedBucket].iterator();
        if (next.hasNext()) {
          nextIteratedBucket++;
          break;
        } else {
          next = null;
        }
      }
    }
    return next;
  }

  @Override
  public void close() {
    if (buckets != null) {
      for (UnlimitedMappingCache cache : buckets) {
        if (cache != null) {
          cache.close();
        }
      }
    }
  }

}
