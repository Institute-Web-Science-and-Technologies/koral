package de.uni_koblenz.west.cidre.slave.triple_store.loader.impl;

import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.fileTransfer.FileReceiver;
import de.uni_koblenz.west.cidre.common.fileTransfer.FileReceiverConnection;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.networManager.MessageNotifier;
import de.uni_koblenz.west.cidre.slave.triple_store.TripleStoreAccessor;
import de.uni_koblenz.west.cidre.slave.triple_store.loader.GraphChunkListener;

public class GraphChunkLoader implements GraphChunkListener {

	private final Logger logger;

	private final int slaveID;

	private final File workingDir;

	private final FileReceiver receiver;

	private final FileReceiverConnection connection;

	private final TripleStoreAccessor tripleStore;

	private final MessageNotifier messageNotifier;

	public GraphChunkLoader(int slaveID, File workingDir,
			FileReceiverConnection connection, TripleStoreAccessor tripleStore,
			MessageNotifier messageNotifier, Logger logger) {
		this.logger = logger;
		this.slaveID = slaveID;
		this.tripleStore = tripleStore;
		this.connection = connection;
		this.workingDir = workingDir;
		this.messageNotifier = messageNotifier;
		if (workingDir.exists()) {
			deleteContent(workingDir);
		} else {
			if (!workingDir.mkdirs()) {
				throw new RuntimeException(
						"The working directory " + workingDir.getAbsolutePath()
								+ " could not be created!");
			}
		}
		receiver = new FileReceiver(workingDir, slaveID, connection, 1,
				new String[] { "enc.gz" }, logger);
	}

	@Override
	public void processMessage(byte[][] message) {
		if (message == null || message.length == 0) {
			return;
		}
		try {
			MessageType mType = null;
			try {
				mType = MessageType.valueOf(message[0][0]);
				switch (mType) {
				case START_FILE_TRANSFER:
					long totalNumberOfChunks = ByteBuffer.wrap(message[1])
							.getLong();
					if (totalNumberOfChunks < FileReceiver.NUMBER_OF_PARALLELY_REQUESTED_FILE_CHUNKS) {
						receiver.adjustMaximalNumberOfParallelRequests(
								(int) totalNumberOfChunks);
					}
					receiver.requestFiles();
					break;
				case FILE_CHUNK_RESPONSE:
					int fileID = ByteBuffer.wrap(message[1]).getInt();
					long chunkID = ByteBuffer.wrap(message[2]).getLong();
					totalNumberOfChunks = ByteBuffer.wrap(message[3]).getLong();
					byte[] chunkContent = message[4];
					try {
						receiver.receiveFileChunk(fileID, chunkID,
								totalNumberOfChunks, chunkContent);
						if (receiver.isFinished()) {
							receiver.close();
							loadGraphChunk();
						}
					} catch (IOException e) {
						if (logger != null) {
							logger.finer(
									"error during receiving a graph chunk");
							logger.throwing(e.getStackTrace()[0].getClassName(),
									e.getStackTrace()[0].getMethodName(), e);
						}
						connection.sendFailNotification(slaveID,
								e.getMessage());
						close();
					}
					break;
				default:
					if (logger != null) {
						logger.finer(
								"Unsupported message type: " + mType.name());
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
		} catch (RuntimeException e) {
			if (logger != null) {
				logger.throwing(e.getStackTrace()[0].getClassName(),
						e.getStackTrace()[0].getMethodName(), e);
			}
			connection.sendFailNotification(slaveID, e.getMessage());
			close();
		}
	}

	private void loadGraphChunk() {
		// TODO Auto-generated method stub

		// TODO remove
		if (logger != null) {
			logger.info("Now I throw an exception.");
		}

		// TODO remove
		throw new RuntimeException("This is a test exception");

		// connection.sendFinish(slaveID);
		// close();
	}

	@Override
	public int getSlaveID() {
		return slaveID;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		messageNotifier.unregisterMessageListener(GraphChunkListener.class,
				this);
		receiver.close();
		deleteContent(workingDir);
		workingDir.delete();
	}

	private void deleteContent(File workingDir) {
		for (File containedFile : workingDir.listFiles()) {
			containedFile.delete();
		}
	}

}
