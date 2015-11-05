package de.uni_koblenz.west.cidre.common.query.messagePassing;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;

public class MessageSenderBuffer implements Closeable {

	private final Logger logger;

	private final MessageSender messageSender;

	public MessageSenderBuffer(MessageSender messageSender, Logger logger) {
		this.logger = logger;
		this.messageSender = messageSender;
	}

	public void sendQueryCreate(byte[] queryTree) {
		// TODO Auto-generated method stub
	}

	public void sendQueryCreated(int receiver, int queryID) {
		sendQueryCreated(receiver,
				ByteBuffer.allocate(4).putInt(queryID).array(), 0);
	}

	public void sendQueryCreated(int receiver, byte[] array,
			int startIndexOfQueryID) {
		// TODO Auto-generated method stub

	}

	public void sendQueryMapping(Mapping mapping, byte[] receiverTaskID,
			int receiverTaskIDOffset, MappingRecycleCache mappingCache) {

		// TODO if receiver is another computer
		mappingCache.releaseMapping(mapping);
	}

	public void sendQueryStart(int queryID) {
		// TODO Auto-generated method stub
	}

	/**
	 * Broadcasts the finish message to all instances of this query task on the
	 * other computers
	 * 
	 * @param finishedTaskID
	 * @param finishedTaskIDOffset
	 */
	public void sendQueryTaskFinished(byte[] finishedTaskID,
			int finishedTaskIDOffset) {
		// TODO Auto-generated method stub

	}

	public void sendAllBufferedMessages() {
		// TODO Auto-generated method stub

	}

	public void sendQueryTaskFailed(int receiver, long taskId, String message) {
		// TODO Auto-generated method stub

	}

	public void sendQueryAbortion(int queryID) {
		// TODO Auto-generated method stub
	}

	public void clear() {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
