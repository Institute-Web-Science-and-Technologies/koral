package de.uni_koblenz.west.cidre.common.fileTransfer;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FileSender implements Closeable, AutoCloseable {

	private final List<File> files;

	private final FileSetChunkReader reader;

	private final FileSenderConnection connection;

	public FileSender(List<File> files, FileSenderConnection connection) {
		this.files = files;
		reader = new FileSetChunkReader();
		this.connection = connection;
	}

	public FileSender(File file, FileSenderConnection connection) {
		files = new ArrayList<>();
		files.add(file);
		reader = new FileSetChunkReader();
		this.connection = connection;
	}

	public void sendFile(int slaveID, File file) {
		connection.sendFileLength(slaveID,
				reader.getNumberOfChunksInFile(file));

	}

	public FileChunk sendFileChunk(int slaveID, int fileID, long chunkID) {
		FileChunk fileChunk = reader.getFileChunk(files.get(fileID), fileID,
				chunkID);
		connection.sendFileChunk(slaveID, fileChunk);
		return fileChunk;
	}

	@Override
	public void close() {
		if (reader != null) {
			reader.close();
		}
	}

}
