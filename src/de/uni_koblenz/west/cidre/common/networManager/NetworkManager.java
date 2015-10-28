package de.uni_koblenz.west.cidre.common.networManager;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.logging.Logger;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.fileTransfer.FileChunk;
import de.uni_koblenz.west.cidre.common.fileTransfer.FileReceiverConnection;
import de.uni_koblenz.west.cidre.common.fileTransfer.FileSenderConnection;
import de.uni_koblenz.west.cidre.common.messages.MessageType;

/**
 * First slave has id 1!!
 */
public class NetworkManager
		implements Closeable, FileSenderConnection, FileReceiverConnection {

	private final ZContext context;

	private Socket receiver;

	private final Socket[] senders;

	private int currentID;

	public NetworkManager(Configuration conf, Logger logger,
			String[] currentServer) {
		context = NetworkContextFactory.getNetworkContext();

		receiver = context.createSocket(ZMQ.PULL);
		receiver.bind("tcp://" + currentServer[0] + ":" + currentServer[1]);

		if (logger != null) {
			logger.info("network manager listening on tcp://" + currentServer[0]
					+ ":" + currentServer[1]);
		}

		senders = new Socket[conf.getNumberOfSlaves() + 1];
		String[] master = conf.getMaster();
		senders[0] = context.createSocket(ZMQ.PUSH);

		senders[0].connect("tcp://" + master[0] + ":" + master[1]);
		if (Arrays.equals(currentServer, master)) {
			currentID = 0;
		}
		for (int i = 1; i < senders.length; i++) {
			String[] slave = conf.getSlave(i - 1);
			senders[i] = context.createSocket(ZMQ.PUSH);
			senders[i].connect("tcp://" + slave[0] + ":" + slave[1]);
			if (Arrays.equals(currentServer, slave)) {
				currentID = i;
			}
		}
	}

	public void send(int receiver, byte[] message) {
		Socket out = senders[receiver];
		if (out != null) {
			out.send(message);
		}
	}

	public void broadcastToAllOtherSlaves(byte[] message) {
		for (int i = 1; i < senders.length; i++) {
			if (i == currentID) {
				// do not broadcast a message to oneself
				continue;
			}
			Socket out = senders[i];
			if (out != null) {
				out.send(message);
			}
		}
	}

	public byte[] receive() {
		if (receiver != null) {
			return receiver.recv(ZMQ.DONTWAIT);
		} else {
			return null;
		}
	}

	public int getNumberOfSlaves() {
		return senders.length - 1;
	}

	@Override
	public void requestFileChunk(int clientID, int fileID, FileChunk chunk) {
		byte[] request = new byte[1 + 4 + 8];
		request[0] = MessageType.FILE_CHUNK_REQUEST.getValue();
		byte[] fileIDBytes = ByteBuffer.allocate(4).putInt(fileID).array();
		System.arraycopy(fileIDBytes, 0, request, 1, fileIDBytes.length);
		byte[] chunkID = ByteBuffer.allocate(8)
				.putLong(chunk.getSequenceNumber()).array();
		System.arraycopy(chunkID, 0, request, 5, chunkID.length);
		send(clientID, request);
		chunk.setRequestTime(System.currentTimeMillis());
	}

	@Override
	public void sendFileChunk(int slaveID, FileChunk fileChunk) {
		senders[slaveID].sendMore(
				new byte[] { MessageType.FILE_CHUNK_RESPONSE.getValue() });
		senders[slaveID].sendMore(
				ByteBuffer.allocate(4).putInt(fileChunk.getFileID()).array());
		senders[slaveID].sendMore(ByteBuffer.allocate(8)
				.putLong(fileChunk.getSequenceNumber()).array());
		senders[slaveID].sendMore(ByteBuffer.allocate(8)
				.putLong(fileChunk.getTotalNumberOfSequences()).array());
		senders[slaveID].send(fileChunk.getContent());
	}

	@Override
	public void sendFileLength(int slaveID, long totalNumberOfFileChunks) {
		byte[] message = ByteBuffer.allocate(9)
				.put(MessageType.START_FILE_TRANSFER.getValue())
				.putLong(totalNumberOfFileChunks).array();
		senders[slaveID].send(message);
	}

	@Override
	public void close() {
		for (int i = 0; i < senders.length; i++) {
			context.destroySocket(senders[i]);
			senders[i] = null;
		}
		context.destroySocket(receiver);
		receiver = null;
		NetworkContextFactory.destroyNetworkContext(context);
	}

}
