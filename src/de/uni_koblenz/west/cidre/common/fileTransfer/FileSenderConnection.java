package de.uni_koblenz.west.cidre.common.fileTransfer;

public interface FileSenderConnection {

	public void sendFileChunk(int slaveID, FileChunk fileChunk);

}
