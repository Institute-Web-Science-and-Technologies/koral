package de.uni_koblenz.west.cidre.common.query.messagePassing;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;

public class MessageSenderBuffer implements Closeable {

	private final Logger logger;

	private final MessageSender messageSender;

	private final Mapping[][] buffer;

	private final int[] nextIndex;

	public MessageSenderBuffer(int numberOfSlaves, int bundleSize,
			MessageSender messageSender, Logger logger) {
		this.logger = logger;
		this.messageSender = messageSender;
		buffer = new Mapping[numberOfSlaves + 1][bundleSize];
		nextIndex = new int[numberOfSlaves + 1];
	}

	public void sendQueryCreate(int queryId, byte[] queryTree) {
		ByteBuffer message = ByteBuffer.allocate(queryTree.length + 5);
		message.put(MessageType.QUERY_CREATE.getValue()).putInt(queryId)
				.put(queryTree);
		messageSender.sendToAllSlaves(message.array());
	}

	public void sendQueryCreated(int receiver, long rootTaskID) {
		ByteBuffer message = ByteBuffer.allocate(11);
		message.put(MessageType.QUERY_CREATED.getValue())
				.putShort((short) messageSender.getCurrentID())
				.putLong(rootTaskID);
		messageSender.send(receiver, message.array());
	}

	public void sendQueryStart(int queryID) {
		ByteBuffer message = ByteBuffer.allocate(5);
		message.put(MessageType.QUERY_CREATE.getValue()).putInt(queryID);
		messageSender.sendToAllSlaves(message.array());
	}

	public void sendQueryMapping(Mapping mapping, byte[] receiverTaskID,
			int receiverTaskIDOffset, MappingRecycleCache mappingCache) {

		// TODO if receiver is another computer
		mappingCache.releaseMapping(mapping);
	}

	/**
	 * Broadcasts the finish message to all instances of this query task on the
	 * other computers. If the parent task is the root task, it is also sent to
	 * the root
	 * 
	 * @param finishedTaskID
	 * @param parentTaskID
	 * @param rootID
	 */
	public void sendQueryTaskFinished(long finishedTaskID, long parentTaskID,
			long rootID) {
		sendAllBufferedMessages();
		ByteBuffer message = ByteBuffer.allocate(11);
		message.put(MessageType.QUERY_TASK_FINISHED.getValue())
				.putShort((short) messageSender.getCurrentID())
				.putLong(finishedTaskID);
		messageSender.sendToAllOtherSlaves(message.array());
		if (parentTaskID == rootID) {
			messageSender.send(getComputerID(rootID), message.array());
		}
	}

	private int getComputerID(long taskID) {
		return (int) (taskID >>> 6 * 8);
	}

	public void sendAllBufferedMessages() {
		// TODO Auto-generated method stub

	}

	private void sendBufferedMessages(int receiver) {
		synchronized (buffer[receiver]) {
			// TODO Auto-generated method stub
		}
	}

	public void sendQueryTaskFailed(int receiver, long failedTaskId,
			long rootID, String message) {
		// TODO Auto-generated method stub

	}

	public void sendQueryAbortion(int queryID) {
		// TODO Auto-generated method stub
	}

	public void clear() {
		int bufferSize = buffer[0].length;
		for (int i = 0; i < buffer.length; i++) {
			synchronized (buffer[i]) {
				buffer[i] = new Mapping[bufferSize];
				nextIndex[i] = 0;
			}
		}
	}

	@Override
	public void close() {
		sendAllBufferedMessages();
	}

}
