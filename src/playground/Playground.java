package playground;

import java.io.File;

import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;
import de.uni_koblenz.west.cidre.common.utils.CachedFileReceiverQueue;

/**
 * A class to test source code. Not used within CIDRE.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class Playground {

	public static void main(String[] args) {
		MappingRecycleCache recycleCache = new MappingRecycleCache(100);
		CachedFileReceiverQueue queue = new CachedFileReceiverQueue(3,
				new File("/home/danijank/Downloads/test"), 0);
		enqueue(queue, 1);
		enqueue(queue, 2);
		enqueue(queue, 3);
		enqueue(queue, 4);
		dequeue(recycleCache, queue);
		dequeue(recycleCache, queue);
		dequeue(recycleCache, queue);
		enqueue(queue, 5);
		enqueue(queue, 6);
		enqueue(queue, 7);
		enqueue(queue, 8);
		dequeue(recycleCache, queue);
		dequeue(recycleCache, queue);
		dequeue(recycleCache, queue);
		dequeue(recycleCache, queue);
		dequeue(recycleCache, queue);
		enqueue(queue, 9);
		dequeue(recycleCache, queue);
		queue.close();
	}

	private static void enqueue(CachedFileReceiverQueue queue, int value) {
		System.out.println("enqueue " + value);
		queue.enqueue(new byte[] { (byte) value }, 0, 1);
	}

	private static void dequeue(MappingRecycleCache recycleCache,
			CachedFileReceiverQueue queue) {
		System.out.println("dequeue");
		queue.dequeue(recycleCache);
	}

}
