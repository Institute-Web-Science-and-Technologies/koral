package de.uni_koblenz.west.cidre.master.networkManager;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.fileTransfer.FileChunk;
import de.uni_koblenz.west.cidre.common.fileTransfer.FileSenderConnection;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.networManager.NetworkManager;
import de.uni_koblenz.west.cidre.common.utils.NumberConversion;
import de.uni_koblenz.west.cidre.master.CidreMaster;

import java.nio.ByteBuffer;

/**
 * Implementation of network manager methods specific for {@link CidreMaster}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MasterNetworkManager extends NetworkManager implements FileSenderConnection {

  public MasterNetworkManager(Configuration conf, String[] currentServer) {
    super(conf, currentServer);
  }

  @Override
  public void sendFileChunk(int slaveID, FileChunk fileChunk) {
    boolean wasMessageSent = sendMore(slaveID,
            new byte[] { MessageType.FILE_CHUNK_RESPONSE.getValue() }, false);
    if (!wasMessageSent) {
      return;
    }
    wasMessageSent = sendMore(slaveID, NumberConversion.int2bytes(fileChunk.getFileID()), false);
    if (!wasMessageSent) {
      return;
    }
    wasMessageSent = sendMore(slaveID, NumberConversion.long2bytes(fileChunk.getSequenceNumber()),
            false);
    if (!wasMessageSent) {
      return;
    }
    wasMessageSent = sendMore(slaveID,
            NumberConversion.long2bytes(fileChunk.getTotalNumberOfSequences()), false);
    if (!wasMessageSent) {
      return;
    }
    send(slaveID, fileChunk.getContent(), false);
  }

  @Override
  public void sendFileLength(int slaveID, long totalNumberOfFileChunks) {
    byte[] message = ByteBuffer.allocate(Byte.BYTES + Long.BYTES)
            .put(MessageType.START_FILE_TRANSFER.getValue()).putLong(totalNumberOfFileChunks)
            .array();
    send(slaveID, message);
  }

}
