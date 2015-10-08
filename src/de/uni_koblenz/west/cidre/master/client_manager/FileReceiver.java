package de.uni_koblenz.west.cidre.master.client_manager;

import java.io.Closeable;

public class FileReceiver implements Closeable {

	private final int clientID;

	private final ClientConnectionManager clientConnections;

	private final int totalNumberOfFiles;

	// TODO handle connection terminations;

	public FileReceiver(int clientID, ClientConnectionManager clientConnections,
			int numberOfFiles) {
		this.clientID = clientID;
		this.clientConnections = clientConnections;
		totalNumberOfFiles = numberOfFiles;
	}

	public void requestFiles() {
		// TODO Auto-generated method stub

	}

	public void receiveFileChunk(int fileID, long chunkID,
			long totalNumberOfChunks, byte[] chunkContent) {
		// TODO Auto-generated method stub

	}

	public boolean isFinished() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
