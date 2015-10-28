package de.uni_koblenz.west.cidre.slave.networkManager;

import java.nio.ByteBuffer;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.fileTransfer.FileChunk;
import de.uni_koblenz.west.cidre.common.fileTransfer.FileReceiverConnection;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.messages.MessageUtils;
import de.uni_koblenz.west.cidre.common.networManager.NetworkManager;

public class SlaveNetworkManager extends NetworkManager
		implements FileReceiverConnection {

	public SlaveNetworkManager(Configuration conf, String[] currentServer) {
		super(conf, currentServer);
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
		send(0, request);
		chunk.setRequestTime(System.currentTimeMillis());
	}

	@Override
	public void sendFinish(int clientID) {
		byte[] message = ByteBuffer.allocate(3)
				.put(MessageType.GRAPH_LOADING_COMPLETE.getValue())
				.putShort((short) clientID).array();
		send(0, message);
	}

	@Override
	public void sendFailNotification(int slaveID, String message) {
		byte[] messageBytes = MessageUtils.createStringMessage(
				MessageType.GRAPH_LOADING_FAILED,
				"Graph loading faile on slave " + slaveID + ". Cause: "
						+ message,
				null);
		byte[] messageB = ByteBuffer.allocate(2 + messageBytes.length)
				.put(messageBytes[0]).putShort((short) slaveID)
				.put(messageBytes, 1, messageBytes.length - 1).array();
		send(0, messageB);
	}

}
