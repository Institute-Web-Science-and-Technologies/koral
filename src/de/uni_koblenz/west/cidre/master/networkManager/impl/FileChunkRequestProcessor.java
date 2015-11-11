package de.uni_koblenz.west.cidre.master.networkManager.impl;

import java.io.File;
import java.nio.BufferUnderflowException;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.fileTransfer.FileSender;
import de.uni_koblenz.west.cidre.common.fileTransfer.FileSenderConnection;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.messages.MessageUtils;
import de.uni_koblenz.west.cidre.common.utils.NumberConversion;
import de.uni_koblenz.west.cidre.master.networkManager.FileChunkRequestListener;
import de.uni_koblenz.west.cidre.slave.CidreSlave;

/**
 * Sends the requested file chunks to the requesting {@link CidreSlave}s.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class FileChunkRequestProcessor implements FileChunkRequestListener {

	private final Logger logger;

	private final int slaveID;

	private FileSender fileSender;

	private boolean isGraphLoadingComplete;

	private String errorMessage;

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
				short slaveID = NumberConversion.bytes2short(message[1]);
				int fileID = NumberConversion.bytes2int(message[2]);
				long chunkID = NumberConversion.bytes2long(message[3]);
				fileSender.sendFileChunk(slaveID, fileID, chunkID);
				break;
			case GRAPH_LOADING_FAILED:
				slaveID = NumberConversion.bytes2short(message[1]);
				errorMessage = MessageUtils.convertToString(message[2], null);
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

	@Override
	public void processMessage(byte[] message) {
		throw new UnsupportedOperationException();
	}

	public boolean isFailed() {
		return errorMessage != null;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public boolean isFinished() {
		return isGraphLoadingComplete;
	}

	@Override
	public void close() {
		fileSender.close();
	}

}
