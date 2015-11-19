package de.uni_koblenz.west.cidre.common.utils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;

/**
 * Caches received mappings until a limit is reached. Thereafter, mappings are
 * written to files.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class CachedFileReceiverQueue implements Closeable {

	private static final int NUMBER_OF_ENTRIES_PER_CACHE_ROW = 100;

	private final int maxCacheSize;

	private final File cacheDirectory;

	private final byte[][][] messageCache;

	private final int[][] firstIndexCache;

	private final File fileBuffer1;

	private OutputStream fileOutput1;

	private InputStream fileInput1;

	private final File fileBuffer2;

	private OutputStream fileOutput2;

	private InputStream fileInput2;

	// TODO queues have to be synchronized

	public CachedFileReceiverQueue(int maxCacheSize, File cacheDirectory,
			int queueId) {
		this.maxCacheSize = maxCacheSize;
		this.cacheDirectory = cacheDirectory;
		int numberOfRows = maxCacheSize / NUMBER_OF_ENTRIES_PER_CACHE_ROW;
		if (maxCacheSize % NUMBER_OF_ENTRIES_PER_CACHE_ROW > 0) {
			numberOfRows++;
		}
		messageCache = new byte[numberOfRows][][];
		firstIndexCache = new int[numberOfRows][];
		if (!this.cacheDirectory.exists()) {
			this.cacheDirectory.mkdirs();
		}
		fileBuffer1 = new File(this.cacheDirectory.getAbsolutePath()
				+ File.separatorChar + "queue" + queueId + "buffer1");
		fileBuffer2 = new File(this.cacheDirectory.getAbsolutePath()
				+ File.separatorChar + "queue" + queueId + "buffer2");
		// TODO Auto-generated constructor stub
	}

	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}

	public long size() {
		// TODO Auto-generated method stub
		return 0;
	}

	public void enqueue(byte[] message, int firstIndex) {
		// TODO Auto-generated method stub

	}

	public Mapping dequeue(MappingRecycleCache recycleCache) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		try {
			if (fileInput1 != null) {
				fileInput1.close();
			} else if (fileOutput1 != null) {
				fileOutput1.close();
			}
			if (fileInput2 != null) {
				fileInput2.close();
			} else if (fileOutput2 != null) {
				fileOutput2.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		deleteCacheDirectory();
	}

	private void deleteCacheDirectory() {
		if (!cacheDirectory.exists()) {
			return;
		}
		fileBuffer1.delete();
		fileBuffer2.delete();
		cacheDirectory.delete();
	}

}
