package de.uni_koblenz.west.cidre.master.client_manager;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.PriorityQueue;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.messages.MessageType;

public class FileReceiver implements Closeable {

	public static final int NUMBER_OF_PARALLELY_REQUESTED_FILE_CHUNKS = 10;

	public static final int FILE_CHUNK_REQUEST_TIMEOUT = 1000;

	private final Logger logger;

	private final int clientID;

	private final ClientConnectionManager clientConnections;

	private final File workingDir;

	private final int totalNumberOfFiles;

	private int currentFile;

	private OutputStream out;

	private final PriorityQueue<FileChunk> unprocessedChunks;

	public FileReceiver(File workingDir, int clientID,
			ClientConnectionManager clientConnections, int numberOfFiles,
			Logger logger) {
		this.workingDir = workingDir;
		this.clientID = clientID;
		this.clientConnections = clientConnections;
		totalNumberOfFiles = numberOfFiles;
		this.logger = logger;
		unprocessedChunks = new PriorityQueue<>(
				NUMBER_OF_PARALLELY_REQUESTED_FILE_CHUNKS);
	}

	public void requestFiles() {
		currentFile = -1;
		requestNextFile();
	}

	private void requestNextFile() {
		if (out != null) {
			close();
		}
		currentFile++;
		if (currentFile < totalNumberOfFiles) {
			if (logger != null) {
				logger.finest("Requesting file " + currentFile + " from client "
						+ clientID + ".");
			}
			try {
				out = new BufferedOutputStream(new FileOutputStream(
						new File(workingDir.getAbsolutePath()
								+ File.separatorChar + currentFile)));
			} catch (FileNotFoundException e) {
				if (logger != null) {
					logger.throwing(e.getStackTrace()[0].getClassName(),
							e.getStackTrace()[0].getMethodName(), e);
				}
			}
			requestNextFileChunk();
		}
	}

	private void requestNextFileChunk() {
		FileChunk chunk = unprocessedChunks.peek();
		if (chunk == null) {
			chunk = new FileChunk(currentFile, 0);
			unprocessedChunks.add(chunk);
			requestFileChunk(chunk);
		} else if (!chunk.isReceived()) {
			// there are file chunks that have not been received yet
			// request them again
			requestFileChunk(chunk);
		}
		while (unprocessedChunks
				.size() < NUMBER_OF_PARALLELY_REQUESTED_FILE_CHUNKS) {
			if (chunk.isLastChunk()) {
				break;
			}
			chunk = new FileChunk(currentFile, chunk.getSequenceNumber() + 1,
					chunk.getTotalNumberOfSequences());
			unprocessedChunks.add(chunk);
			requestFileChunk(chunk);
		}
	}

	private void requestFileChunk(FileChunk chunk) {
		byte[] request = new byte[1 + 4 + 8];
		request[0] = MessageType.REQUEST_FILE_CHUNK.getValue();
		byte[] fileID = ByteBuffer.allocate(4).putInt(currentFile).array();
		System.arraycopy(fileID, 0, request, 1, fileID.length);
		byte[] chunkID = ByteBuffer.allocate(8)
				.putLong(chunk.getSequenceNumber()).array();
		System.arraycopy(chunkID, 0, request, 5, chunkID.length);
		clientConnections.send(clientID, request);
		chunk.setRequestTime(System.currentTimeMillis());
	}

	public void receiveFileChunk(int fileID, long chunkID,
			long totalNumberOfChunks, byte[] chunkContent) throws IOException {
		assert!unprocessedChunks.isEmpty();
		// update content of file chunks
		for (FileChunk chunk : unprocessedChunks) {
			if (chunk.getFileID() != fileID) {
				continue;
			}
			if (chunk.getSequenceNumber() == chunkID) {
				chunk.setContent(chunkContent);
			}
			if (chunk.getTotalNumberOfSequences() == 0) {
				chunk.setTotalNumberOfSequences(totalNumberOfChunks);
			}
		}
		// process file chunks
		FileChunk chunk = null;
		boolean fileIsFinished = false;
		do {
			if (fileIsFinished
					&& unprocessedChunks.peek().getFileID() == fileID) {
				// remove further file chunks behind the last file chunk of
				// this file
				unprocessedChunks.poll();
			} else {
				chunk = unprocessedChunks.peek();
				if (chunk.getFileID() < fileID) {
					// remove chunks from old files
					unprocessedChunks.poll();
				} else if (chunk.getFileID() == fileID && chunk.isReceived()) {
					// write all already received file chunks to disk
					// TODO remove
					out.write(("\nchunk " + chunk.getFileID() + "/"
							+ chunk.getTotalNumberOfSequences() + ": ")
									.getBytes());
					out.write(chunk.getContent());
					unprocessedChunks.poll();
					fileIsFinished = chunk.isLastChunk();
				} else {
					break;
				}
			}
		} while (!unprocessedChunks.isEmpty() && chunk.isReceived()
				&& chunk.getFileID() <= fileID);
		if (fileIsFinished) {
			if (logger != null) {
				logger.finest("Received file " + fileID + " from client "
						+ clientID + " completely.");
			}
			requestNextFile();
		} else {
			requestNextFileChunk();
		}
	}

	public boolean isFinished() {
		return currentFile > totalNumberOfFiles - 1;
	}

	@Override
	public void close() {
		if (out != null) {
			try {
				out.close();
			} catch (IOException e) {
				if (logger != null) {
					logger.throwing(e.getStackTrace()[0].getClassName(),
							e.getStackTrace()[0].getMethodName(), e);
				}
			}
		}
	}

}
