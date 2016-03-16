package de.uni_koblenz.west.cidre.common.fileTransfer;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.PriorityQueue;
import java.util.logging.Logger;

/**
 * Requests file chunks from {@link FileSender}. At most
 * {@link #NUMBER_OF_PARALLELY_REQUESTED_FILE_CHUNKS} file chunks are requested
 * in parallel. If a requested file chunk is not received within
 * {@link #FILE_CHUNK_REQUEST_TIMEOUT} seconds, it is requested again. If
 * several files should be received, it only requests file chunks of the next
 * file, if the current file is received completely.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class FileReceiver implements Closeable {

	public static final int NUMBER_OF_PARALLELY_REQUESTED_FILE_CHUNKS = 200;

	public static final int FILE_CHUNK_REQUEST_TIMEOUT = 1000;

	private final Logger logger;

	private final int clientID;

	private final FileReceiverConnection connectionToSender;

	private final File workingDir;

	private final int totalNumberOfFiles;

	private int currentFile;

	private final String[] fileExtensions;

	private OutputStream out;

	private final PriorityQueue<FileChunk> unprocessedChunks;

	private int maxNumberOfParallelRequests;

	public FileReceiver(File workingDir, int clientID, int numberOfSlaves,
			FileReceiverConnection connectionToSender, int numberOfFiles,
			String[] fileExtensions, Logger logger) {
		this.workingDir = workingDir;
		this.clientID = clientID;
		this.connectionToSender = connectionToSender;
		totalNumberOfFiles = numberOfFiles;
		this.fileExtensions = fileExtensions;
		this.logger = logger;
		// maxNumberOfParallelRequests = numberOfSlaves == 1
		// ? NUMBER_OF_PARALLELY_REQUESTED_FILE_CHUNKS : 10;
		maxNumberOfParallelRequests = Math.max(10,
				NUMBER_OF_PARALLELY_REQUESTED_FILE_CHUNKS / numberOfSlaves);
		unprocessedChunks = new PriorityQueue<>(
				NUMBER_OF_PARALLELY_REQUESTED_FILE_CHUNKS);
	}

	public int getMaximalNumberOfParallelRequests() {
		return maxNumberOfParallelRequests;
	}

	public void adjustMaximalNumberOfParallelRequests(int max) {
		maxNumberOfParallelRequests = max;
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
				logger.finest("Requesting file " + currentFile + ".");
			}
			try {
				out = new BufferedOutputStream(
						new FileOutputStream(getFileWithID(currentFile)));
			} catch (FileNotFoundException e) {
				if (logger != null) {
					logger.throwing(e.getStackTrace()[0].getClassName(),
							e.getStackTrace()[0].getMethodName(), e);
				}
			}
			requestNextFileChunk();
		}
	}

	public File getFileWithID(int fileID) {
		return new File(workingDir.getAbsolutePath() + File.separatorChar
				+ fileID + "." + fileExtensions[fileID]);
	}

	private void requestNextFileChunk() {
		FileChunk chunk = unprocessedChunks.peek();
		if (chunk == null) {
			// no chunk has been requested yet
			chunk = new FileChunk(currentFile, 0);
			unprocessedChunks.add(chunk);
			connectionToSender.requestFileChunk(clientID, currentFile, chunk);
		} else {
			// rerequest unreceived chunks
			for (FileChunk fChunk : unprocessedChunks) {
				if (!fChunk.isReceived() && System.currentTimeMillis() - fChunk
						.getRequestTime() > FILE_CHUNK_REQUEST_TIMEOUT) {
					connectionToSender.requestFileChunk(clientID, currentFile,
							fChunk);
				}
				chunk = chunk.compareTo(fChunk) < 0 ? fChunk : chunk;
			}
		}
		// invariant: chunk is the last requested file chunk
		// request additional file chunks
		while (unprocessedChunks.size() < maxNumberOfParallelRequests) {
			if (chunk.isLastChunk()) {
				break;
			}
			chunk = new FileChunk(currentFile, chunk.getSequenceNumber() + 1,
					chunk.getTotalNumberOfSequences());
			unprocessedChunks.add(chunk);
			connectionToSender.requestFileChunk(clientID, currentFile, chunk);
		}
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
				logger.finest("Received file " + fileID + " completely.");
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
