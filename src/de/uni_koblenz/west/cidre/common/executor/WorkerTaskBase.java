package de.uni_koblenz.west.cidre.common.executor;

import java.io.File;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.executor.messagePassing.MessageSenderBuffer;
import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;
import de.uni_koblenz.west.cidre.common.utils.CachedFileReceiverQueue;

/**
 * 
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public abstract class WorkerTaskBase implements WorkerTask {

	protected Logger logger;

	private final long id;

	private CachedFileReceiverQueue[] inputQueues;

	private final int cacheSize;

	private final File cacheDirectory;

	public WorkerTaskBase(long id, int cacheSize, File cacheDirectory) {
		this.id = id;
		this.cacheSize = cacheSize;
		this.cacheDirectory = new File(cacheDirectory.getAbsolutePath()
				+ File.separatorChar + "operator_" + this.id);
	}

	@Override
	public void setUp(MessageSenderBuffer messageSender,
			MappingRecycleCache recycleCache, Logger logger) {
		this.logger = logger;
	}

	@Override
	public long getID() {
		return id;
	}

	protected void addInputQueue() {
		if (inputQueues == null || inputQueues.length == 0) {
			inputQueues = new CachedFileReceiverQueue[1];
		} else {
			CachedFileReceiverQueue[] newInputQueues = new CachedFileReceiverQueue[inputQueues.length
					+ 1];
			for (int i = 0; i < inputQueues.length; i++) {
				newInputQueues[i] = inputQueues[i];
			}
			inputQueues = newInputQueues;
		}
		inputQueues[0] = new CachedFileReceiverQueue(cacheSize, cacheDirectory);
	}

	@Override
	public boolean hasInput() {
		for (CachedFileReceiverQueue queue : inputQueues) {
			if (!queue.isEmpty()) {
				return true;
			}
		}
		return false;
	}

	protected void enqueuMessage(int inputQueueIndex, byte[] message,
			int firstIndex) {
		inputQueues[inputQueueIndex].enqueue(message, firstIndex);
	}

	protected Mapping consumeMapping(int inputQueueIndex,
			MappingRecycleCache recycleCache) {
		return inputQueues[inputQueueIndex].dequeue(recycleCache);
	}

	protected boolean isInputQueueEmpty(int inputQueueIndex) {
		return inputQueues[inputQueueIndex].isEmpty();
	}

	@Override
	public void close() {
		for (CachedFileReceiverQueue queue : inputQueues) {
			queue.close();
		}
	}

	@Override
	public String toString() {
		return getClass().getName() + "[id=" + id + "(slave="
				+ (id >>> (Integer.SIZE + Short.SIZE)) + " query="
				+ ((id << Short.SIZE) >>> (Short.SIZE + Short.SIZE)) + " task="
				+ ((id << (Short.SIZE + Integer.SIZE)) >>> (Short.SIZE
						+ Integer.SIZE))
				+ ")]";
	}

}
