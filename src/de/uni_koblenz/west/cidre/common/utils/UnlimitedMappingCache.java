package de.uni_koblenz.west.cidre.common.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;

/**
 * Stores mappings in memory until limit is reached. Thereafter, mappings are
 * stored on disk.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class UnlimitedMappingCache
		implements Closeable, Iterable<Mapping>, Iterator<Mapping> {

	private final MappingRecycleCache recycleCache;

	private final Mapping[] cache;

	private int nextWriteIndex;

	private final File diskCacheFile;

	private DataOutputStream fileOutput;

	private DataInputStream fileInput;

	private Mapping next;

	private int nextReadIndex;

	public UnlimitedMappingCache(int maxInMemorySize, File cacheDirectory,
			MappingRecycleCache recycleCache, String uniqueFileNameSuffix) {
		super();
		this.recycleCache = recycleCache;
		cache = new Mapping[maxInMemorySize];
		if (!cacheDirectory.exists()) {
			cacheDirectory.mkdirs();
		}
		diskCacheFile = new File(cacheDirectory.getAbsolutePath()
				+ File.separatorChar + "cache" + uniqueFileNameSuffix);
		nextWriteIndex = 0;
	}

	public void append(Mapping mapping) {
		if (nextWriteIndex < cache.length) {
			cache[nextWriteIndex++] = mapping;
		} else {
			try {
				if (fileInput != null) {
					fileInput.close();
				}
				if (fileOutput == null) {
					fileOutput = new DataOutputStream(new BufferedOutputStream(
							new FileOutputStream(diskCacheFile, true)));
				}
				fileOutput.writeInt(mapping.getLengthOfMappingInByteArray());
				fileOutput.write(mapping.getByteArray(),
						mapping.getFirstIndexOfMappingInByteArray(),
						mapping.getLengthOfMappingInByteArray());
				recycleCache.releaseMapping(mapping);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public boolean hasNext() {
		return next != null;
	}

	@Override
	public Mapping next() {
		if (next == null) {
			throw new NoSuchElementException();
		}
		Mapping result = next;
		next = getNext();
		return result;
	}

	@Override
	public Iterator<Mapping> iterator() {
		if (nextWriteIndex >= cache.length) {
			try {
				boolean wasAFileWritten = false;
				if (fileOutput != null) {
					fileOutput.close();
					wasAFileWritten = true;
				}
				if (fileInput != null) {
					fileInput.close();
					wasAFileWritten = true;
				}
				if (wasAFileWritten) {
					fileInput = new DataInputStream(new BufferedInputStream(
							new FileInputStream(diskCacheFile)));
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		nextReadIndex = 0;
		next = getNext();
		return this;
	}

	private Mapping getNext() {
		if (nextReadIndex < nextWriteIndex && nextReadIndex < cache.length) {
			return cache[nextReadIndex++];
		} else if (fileInput != null) {
			try {
				try {
					int lengthOfArray = fileInput.readInt();
					byte[] mappingArray = new byte[lengthOfArray];
					fileInput.readFully(mappingArray);
					return recycleCache.createMapping(mappingArray, 0,
							mappingArray.length);
				} catch (EOFException e) {
					// the complete file has been read
					fileInput.close();
					fileInput = null;
					return null;
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

		} else {
			return null;
		}
	}

	@Override
	public void close() {
		next = null;
		for (int i = 0; i < cache.length; i++) {
			Mapping mapping = cache[i];
			if (mapping != null) {
				recycleCache.releaseMapping(mapping);
				cache[i] = null;
			}
		}
		try {
			if (fileOutput != null) {
				fileOutput.close();
			}
			if (fileInput != null) {
				fileInput.close();
			}
		} catch (IOException e) {
		}
		if (diskCacheFile.exists()) {
			diskCacheFile.delete();
		}
		if (diskCacheFile.getParentFile() != null) {
			diskCacheFile.getParentFile().delete();
		}
		nextReadIndex = 0;
		nextWriteIndex = 0;
	}

}
