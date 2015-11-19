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

	private final int nextWriteIndex;

	private final int nextReadIndex;

	private final File fileBuffer1;

	private OutputStream fileOutput1;

	private InputStream fileInput1;

	private final File fileBuffer2;

	private OutputStream fileOutput2;

	private InputStream fileInput2;

	private long size;

	private QueueStatus status;

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
		nextReadIndex = -1;
		nextWriteIndex = 0;
		if (!this.cacheDirectory.exists()) {
			this.cacheDirectory.mkdirs();
		}
		fileBuffer1 = new File(this.cacheDirectory.getAbsolutePath()
				+ File.separatorChar + "queue" + queueId + "buffer1");
		fileBuffer2 = new File(this.cacheDirectory.getAbsolutePath()
				+ File.separatorChar + "queue" + queueId + "buffer2");
		status = QueueStatus.MEMORY_MEMORY;
		size = 0;
		// TODO Auto-generated constructor stub
	}

	public boolean isEmpty() {
		return size == 0;
	}

	public long size() {
		return size;
	}

	private boolean isMemoryFull() {
		synchronized (messageCache) {
			return unsynch_isMemorFull();
		}
	}

	private boolean unsynch_isMemorFull() {
		if (nextReadIndex == -1 || nextReadIndex == 0) {
			return nextWriteIndex == maxCacheSize;
		} else {
			return nextWriteIndex == nextReadIndex;
		}
	}

	private void enqueueInMemory() {
		synchronized (messageCache) {
			if (status == QueueStatus.CLOSED) {
				throw new IllegalStateException(
						"Queue has already been closed.");
			}
			// TODO check state change at the end!
		}
	}

	public void enqueue(byte[] message, int firstIndex) {
		size++;
		// TODO Auto-generated method stub

	}

	public Mapping dequeue(MappingRecycleCache recycleCache) {
		// TODO Auto-generated method stub
		size--;
		return null;
	}

	@Override
	public void close() {
		synchronized (messageCache) {
			synchronized (fileBuffer1) {
				synchronized (fileBuffer2) {
					status = QueueStatus.CLOSED;
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
					} finally {
						deleteCacheDirectory();
					}
				}
			}
		}
	}

	private void deleteCacheDirectory() {
		if (!cacheDirectory.exists()) {
			return;
		}
		fileBuffer1.delete();
		fileBuffer2.delete();
		cacheDirectory.delete();
	}

	/**
	 * Defines the state of this queue. Each states consist of two letters:
	 * <ol>
	 * <li>the cache written to</li>
	 * <li>the cache read from</li>
	 * </ol>
	 * 
	 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
	 *
	 */
	enum QueueStatus {

		/**
		 * memory is full =&gt; FILE1_MEMORY
		 */
		MEMORY_MEMORY,

		/**
		 * memory is empty =&gt; MEMORY_FILE1
		 */
		FILE1_MEMORY,

		/**
		 * file1 is empty =&gt; MEMORY_MEMORY <br>
		 * memory is full =&gt; FILE2_FILE1
		 */
		MEMORY_FILE1,

		/**
		 * file1 is empty =&gt; FILE2_MEMORY
		 */
		FILE2_FILE1,

		/**
		 * memory is empty =&gt; MEMORY_FILE2
		 */
		FILE2_MEMORY,

		/**
		 * file2 is empty =&gt; MEMORY_MEMORY<br>
		 * memory is full =&gt; FILE1_FILE2
		 */
		MEMORY_FILE2,

		/**
		 * file2 is empty =&gt; FILE1_MEMORY
		 */
		FILE1_FILE2,

		CLOSED;

	}

}