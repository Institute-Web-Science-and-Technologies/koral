package de.uni_koblenz.west.cidre.common.utils;

import java.io.Closeable;
import java.io.File;
import java.util.Iterator;
import java.util.NoSuchElementException;

import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;

/**
 * Hash set that stores mappings in memory. If a limit is reached, mappings are
 * stored on disk.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class UnlimitedMappingHashSet implements Closeable {

	private final UnlimitedMappingCache[] buckets;

	private final int mappingLimitPerCache;

	private final File cacheDirectory;

	private final MappingRecycleCache recycleCache;

	private final String uniqueFileNameSuffix;

	public UnlimitedMappingHashSet(int inMemoryMappingLimit,
			int numberOfHashBuckets, File cacheDirectory,
			MappingRecycleCache recycleCache, String uniqueFileNameSuffix) {
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
	}

	private int getBucketIndex(Mapping mapping, long joinVar) {
		return ((int) mapping.getValue(joinVar)) % buckets.length;
	}

	public void add(Mapping mapping, long joinVar) {
		int bucketIndex = getBucketIndex(mapping, joinVar);
		if (buckets[bucketIndex] == null) {
			buckets[bucketIndex] = new UnlimitedMappingCache(
					mappingLimitPerCache, cacheDirectory, recycleCache,
					uniqueFileNameSuffix + "_bucket" + bucketIndex);
		}
		buckets[bucketIndex].append(mapping);
	}

	public Iterator<Mapping> getMatchCandidates(Mapping mapping, long joinVar) {
		int bucketIndex = getBucketIndex(mapping, joinVar);
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
	public void close() {
		for (UnlimitedMappingCache cache : buckets) {
			if (cache != null) {
				cache.close();
			}
		}
	}

}
