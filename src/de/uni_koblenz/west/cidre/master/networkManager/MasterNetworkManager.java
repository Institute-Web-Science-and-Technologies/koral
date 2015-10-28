package de.uni_koblenz.west.cidre.master.networkManager;

import java.nio.ByteBuffer;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.fileTransfer.FileChunk;
import de.uni_koblenz.west.cidre.common.fileTransfer.FileSenderConnection;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.networManager.NetworkManager;

public class MasterNetworkManager extends NetworkManager
		implements FileSenderConnection {

	public MasterNetworkManager(Configuration conf, String[] currentServer) {
		super(conf, currentServer);
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

}
