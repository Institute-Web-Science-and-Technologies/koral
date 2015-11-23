package de.uni_koblenz.west.cidre.common.utils;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

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

	private final int maxCacheSize;

	private final File cacheDirectory;

	private final byte[][] messageCache;

	private final int[] firstIndexCache;

	private final int[] lengthCache;

	private int nextWriteIndex;

	private int nextReadIndex;

	private final File fileBuffer1;

	private DataOutputStream fileOutput1;

	private DataInputStream fileInput1;

	private final File fileBuffer2;

	private DataOutputStream fileOutput2;

	private DataInputStream fileInput2;

	private long size;

	private QueueStatus status;

	// TODO queues have to be synchronized

	public CachedFileReceiverQueue(int maxCacheSize, File cacheDirectory,
			int queueId) {
		this.maxCacheSize = maxCacheSize;
		this.cacheDirectory = cacheDirectory;
		messageCache = new byte[this.maxCacheSize][];
		firstIndexCache = new int[this.maxCacheSize];
		lengthCache = new int[this.maxCacheSize];
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

	public synchronized boolean isEmpty() {
		return size == 0;
	}

	public synchronized long size() {
		return size;
	}

	private void enqueueInMemory(byte[] message, int firstIndex,
			int lengthOfMessage) {
		if (!status.name().startsWith("MEMORY_")) {
			throw new IllegalStateException(
					"Illegal attempt to write to memory while being in state "
							+ status.name());
		}
		if (isMemoryFull()) {
			throw new RuntimeException(
					"Enqueuing in memory not possible because memory cache has reached its limit.");
		}
		messageCache[nextWriteIndex] = message;
		firstIndexCache[nextWriteIndex] = firstIndex;
		lengthCache[nextWriteIndex] = lengthOfMessage;
		if (nextReadIndex == -1) {
			// this was the first written entry.
			nextReadIndex = nextWriteIndex;
		}
		nextWriteIndex = (nextWriteIndex + 1) % maxCacheSize;
		if (isMemoryFull()) {
			switch (status) {
			case MEMORY_MEMORY:
				status = QueueStatus.FILE1_MEMORY;
				break;
			case MEMORY_FILE1:
				status = QueueStatus.FILE2_FILE1;
				break;
			case MEMORY_FILE2:
				status = QueueStatus.FILE1_FILE2;
				break;
			default:
				break;
			}
		}
	}

	private boolean isMemoryFull() {
		if (nextReadIndex == -1) {
			return nextWriteIndex == maxCacheSize;
		} else {
			return nextWriteIndex == nextReadIndex;
		}
	}

	private Mapping dequeueInMemory(MappingRecycleCache recycleCache) {
		if (!status.name().endsWith("_MEMORY")) {
			throw new IllegalStateException(
					"Illegal attempt to read from memory while being in state "
							+ status.name());
		}
		if (isMemoryEmpty()) {
			throw new RuntimeException(
					"Dequeuing from memory not possible because memory cache is empty.");
		}
		Mapping result = recycleCache.createMapping(messageCache[nextReadIndex],
				firstIndexCache[nextReadIndex], lengthCache[nextReadIndex]);
		messageCache[nextReadIndex] = null;
		firstIndexCache[nextReadIndex] = -1;
		lengthCache[nextReadIndex] = -1;
		nextReadIndex = (nextReadIndex + 1) % maxCacheSize;
		if (nextReadIndex == nextWriteIndex) {
			nextReadIndex = -1;
		}
		if (isMemoryEmpty()) {
			switch (status) {
			case MEMORY_MEMORY:
				// there is nothing to do here
				break;
			case FILE1_MEMORY:
				status = QueueStatus.MEMORY_FILE1;
				if (fileOutput1 != null) {
					try {
						fileOutput1.close();
						fileOutput1 = null;
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
				break;
			case FILE2_MEMORY:
				status = QueueStatus.MEMORY_FILE2;
				if (fileOutput2 != null) {
					try {
						fileOutput2.close();
						fileOutput2 = null;
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
				break;
			default:
				break;
			}
		}
		return result;
	}

	private boolean isMemoryEmpty() {
		return nextReadIndex == -1;
	}

	public synchronized void enqueue(byte[] message, int firstIndex,
			int length) {
		if (status == QueueStatus.CLOSED) {
			throw new IllegalStateException("Queue has already been closed.");
		}
		size++;
		// TODO Auto-generated method stub

	}

	public synchronized Mapping dequeue(MappingRecycleCache recycleCache) {
		if (status == QueueStatus.CLOSED) {
			throw new IllegalStateException("Queue has already been closed.");
		}
		// TODO Auto-generated method stub
		size--;
		return null;
	}

	@Override
	public synchronized void close() {
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

	private void deleteCacheDirectory() {
		if (!cacheDirectory.exists()) {
			return;
		}
		fileBuffer1.delete();
		fileBuffer2.delete();
		cacheDirectory.delete();
	}

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