package de.uni_koblenz.west.cidre.common.fileTransfer;

import java.io.Closeable;
import java.io.File;
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

	public FileChunk sendFileChunk(int fileID, long chunkID) {
		FileChunk fileChunk = reader.getFileChunk(files.get(fileID), fileID,
				chunkID);
		connection.sendFileChunk(fileChunk);
		return fileChunk;
	}

	@Override
	public void close() {
		if (reader != null) {
			reader.close();
		}
	}

}
