package de.uni_koblenz.west.cidre.master.networkManager.impl;

import java.io.File;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.fileTransfer.FileSender;
import de.uni_koblenz.west.cidre.common.fileTransfer.FileSenderConnection;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.master.networkManager.FileChunkRequestListener;

public class FileChunkRequestProcessor implements FileChunkRequestListener {

	private final Logger logger;

	private final int slaveID;

	private FileSender fileSender;

	private boolean isGraphLoadingComplete;

	public FileChunkRequestProcessor(int slaveID, Logger logger) {
		this.slaveID = slaveID;
		this.logger = logger;
		isGraphLoadingComplete = false;
	}

	@Override
	public int getSlaveID() {
		return slaveID;
	}

	public void sendFile(File file, FileSenderConnection fileSenderConnection) {
		fileSender = new FileSender(file, fileSenderConnection);
		fileSender.sendFile(slaveID, file);
	}

	@Override
	public void processMessage(byte[][] message) {
		if (message == null || message.length == 0) {
			return;
		}
		MessageType mType = null;
		try {
			mType = MessageType.valueOf(message[0][0]);
			switch (mType) {
			case GRAPH_LOADING_COMPLETE:
				isGraphLoadingComplete = true;
				break;
			case FILE_CHUNK_REQUEST:
				short slaveID = ByteBuffer.wrap(message[1]).getShort();
				int fileID = ByteBuffer.wrap(message[2]).getInt();
				long chunkID = ByteBuffer.wrap(message[3]).getLong();
				fileSender.sendFileChunk(slaveID, fileID, chunkID);
				break;
			default:
				if (logger != null) {
					logger.finer("Unsupported message type: " + mType.name());
				}
			}
		} catch (IllegalArgumentException e) {
			if (logger != null) {
				logger.finer("Unknown message type: " + message[0][0]);
				logger.throwing(e.getStackTrace()[0].getClassName(),
						e.getStackTrace()[0].getMethodName(), e);
			}
		} catch (BufferUnderflowException | IndexOutOfBoundsException e) {
			if (logger != null) {
				logger.finer("Message of type " + mType + " is too short.");
				logger.throwing(e.getStackTrace()[0].getClassName(),
						e.getStackTrace()[0].getMethodName(), e);
			}
		}
	}

	public boolean isFinished() {
		return isGraphLoadingComplete;
	}

	@Override
	public void close() {
		fileSender.close();
	}

}
