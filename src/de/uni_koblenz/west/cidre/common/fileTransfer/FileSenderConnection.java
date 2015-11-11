package de.uni_koblenz.west.cidre.common.fileTransfer;

/**
 * Methods required by {@link FileSender} to send file chunks.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface FileSenderConnection {

	public void sendFileLength(int slaveID, long totalNumberOfFileChunks);

	public void sendFileChunk(int slaveID, FileChunk fileChunk);

}
