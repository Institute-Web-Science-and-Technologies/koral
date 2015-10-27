package de.uni_koblenz.west.cidre.common.fileTransfer;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.PriorityQueue;

public class FileSetChunkReader implements AutoCloseable, Closeable {

	private final static int CHUNK_SIZE = 8192;

	private final static int CACHE_SIZE = FileReceiver.NUMBER_OF_PARALLELY_REQUESTED_FILE_CHUNKS;

	private final PriorityQueue<FileChunk> chunkCache;

	private InputStream input;

	private File currentlyReadFile;

	/**
	 * may point at a position behind the file ending
	 */
	private long indexOfNextReadByte;

	public FileSetChunkReader() {
		chunkCache = new PriorityQueue<>(CACHE_SIZE);
	}

	public FileChunk getFileChunk(File file, int fileID, long chunkID) {
		FileChunk chunk = getChunkFromCache(fileID, chunkID);
		if (chunk == null) {
			chunk = readChunkFromFile(file, fileID, chunkID);
			if (chunkCache.size() == CACHE_SIZE) {
				chunkCache.poll();
			}
			chunkCache.offer(chunk);
		}
		return chunk;
	}

	private FileChunk getChunkFromCache(int fileID, long chunkID) {
		for (FileChunk chunk : chunkCache) {
			if (chunk.getFileID() == fileID
					&& chunk.getSequenceNumber() == chunkID) {
				return chunk;
			}
		}
		return null;
	}

	private FileChunk readChunkFromFile(File file, int fileID, long chunkID) {
		long numberOfChunksInFile = getNumberOfChunksInFile(file);
		FileChunk chunk = new FileChunk(fileID, chunkID, numberOfChunksInFile);
		if (chunkID < numberOfChunksInFile) {
			adjustInput(file, chunkID * CHUNK_SIZE);
			chunk.setContent(readContent());
		} else {
			// read from behind the file ending
			chunk.setContent(new byte[0]);
		}
		return chunk;
	}

	private byte[] readContent() {
		byte[] chunkContent = new byte[CHUNK_SIZE];
		try {
			int numberOfReadBytes = input.read(chunkContent);
			if (numberOfReadBytes == -1) {
				return new byte[0];
			} else {
				indexOfNextReadByte += numberOfReadBytes;
				if (numberOfReadBytes == CHUNK_SIZE) {
					return chunkContent;
				} else {
					byte[] result = new byte[numberOfReadBytes];
					System.arraycopy(chunkContent, 0, result, 0,
							numberOfReadBytes);
					return result;
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void adjustInput(File file,
			long indexOfFirstRequestedChunkByteInFile) {
		// ensure that input reads from file
		if (currentlyReadFile == null || !currentlyReadFile.equals(file)) {
			adjustInputStreamToFile(file);
		}
		// ensure that input reads the correct position
		try {
			input.skip(
					indexOfFirstRequestedChunkByteInFile - indexOfNextReadByte);
			indexOfNextReadByte = indexOfFirstRequestedChunkByteInFile;
		} catch (IOException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	private void adjustInputStreamToFile(File file) {
		if (input != null) {
			try {
				input.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			input = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		indexOfNextReadByte = 0;
		currentlyReadFile = file;
	}

	private long getNumberOfChunksInFile(File file) {
		long fileLength = file.length();
		long numberOfChunks = fileLength / CHUNK_SIZE;
		if (fileLength % CHUNK_SIZE != 0) {
			numberOfChunks++;
		}
		return numberOfChunks;
	}

	@Override
	public void close() {
		if (input != null) {
			try {
				input.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
