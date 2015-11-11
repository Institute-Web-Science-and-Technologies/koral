package de.uni_koblenz.west.cidre.common.fileTransfer;

/**
 * Methods required by {@link FileReceiver} to request file chunks.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface FileReceiverConnection {

	public void requestFileChunk(int clientID, int fileID, FileChunk chunk);

	public void sendFinish(int clientID);

	public void sendFailNotification(int slaveID, String message);

}
