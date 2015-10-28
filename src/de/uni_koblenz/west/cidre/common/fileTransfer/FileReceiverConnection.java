package de.uni_koblenz.west.cidre.common.fileTransfer;

public interface FileReceiverConnection {

	public void requestFileChunk(int clientID, int fileID, FileChunk chunk);

	public void sendFinish(int clientID);

	public void sendFailNotification(int slaveID, String message);

}
