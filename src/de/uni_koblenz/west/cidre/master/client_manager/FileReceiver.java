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
			// no chunk has been requested yet
			chunk = new FileChunk(currentFile, 0);
			unprocessedChunks.add(chunk);
			requestFileChunk(chunk);
		} else {
			// rerequest unreceived chunks
			for (FileChunk fChunk : unprocessedChunks) {
				if (!fChunk.isReceived() && System.currentTimeMillis() - fChunk
						.getRequestTime() > FILE_CHUNK_REQUEST_TIMEOUT) {
					requestFileChunk(fChunk);
				}
				chunk = chunk.compareTo(fChunk) < 0 ? fChunk : chunk;
			}
		}
		// invariant: chunk is the last requested file chunk
		// request additional file chunks
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
		// TODO remove
		// try {
		// out.write(("\nchunk " + chunk.getSequenceNumber() + "/"
		// + (chunk.getTotalNumberOfSequences() - 1) + " requested")
		// .getBytes());
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
	}

	public void receiveFileChunk(int fileID, long chunkID,
			long totalNumberOfChunks, byte[] chunkContent) throws IOException {
		assert !unprocessedChunks.isEmpty();
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
		FileChunk chunk = unprocessedChunks.peek();
		// remove chunks of old files
		while (chunk != null && chunk.getFileID() < fileID) {
			unprocessedChunks.poll();
			chunk = unprocessedChunks.peek();
		}
		// write out already received chunks
		while (chunk != null && chunk.getFileID() == fileID
				&& chunk.isReceived()) {
			// TODO remove
			// out.write(("\nchunk " + chunk.getSequenceNumber() + "/"
			// + (chunk.getTotalNumberOfSequences() - 1) + ": ")
			// .getBytes());
			out.write(chunk.getContent());
			unprocessedChunks.poll();
			if (chunk.isLastChunk()) {
				break;
			}
			chunk = unprocessedChunks.peek();
		}
		if (chunk != null && chunk.isReceived() && chunk.isLastChunk()) {
			// delete all further file chunks of this file
			FileChunk first = unprocessedChunks.peek();
			while (first != null && first.getFileID() == fileID) {
				unprocessedChunks.poll();
				first = unprocessedChunks.peek();
			}
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
