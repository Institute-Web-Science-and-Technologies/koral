package de.uni_koblenz.west.cidre.slave.triple_store.loader.impl;

import de.uni_koblenz.west.cidre.common.fileTransfer.FileReceiver;
import de.uni_koblenz.west.cidre.common.fileTransfer.FileReceiverConnection;
import de.uni_koblenz.west.cidre.common.messages.MessageNotifier;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.utils.NumberConversion;
import de.uni_koblenz.west.cidre.master.CidreMaster;
import de.uni_koblenz.west.cidre.slave.triple_store.TripleStoreAccessor;
import de.uni_koblenz.west.cidre.slave.triple_store.loader.GraphChunkListener;

import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Requests the corresponding graph chunk from {@link CidreMaster}. If the chunk
 * is received completely, it is loaded into the local triple store.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class GraphChunkLoader extends Thread implements GraphChunkListener {

  private final Logger logger;

  private final int slaveID;

  private final File workingDir;

  private final FileReceiver receiver;

  private final FileReceiverConnection connection;

  private final Set<File> receivedGraphChunks;

  private final TripleStoreAccessor tripleStore;

  private final MessageNotifier messageNotifier;

  public GraphChunkLoader(int slaveID, int numberOfSlaves, File workingDir,
          FileReceiverConnection connection, TripleStoreAccessor tripleStore,
          MessageNotifier messageNotifier, Logger logger) {
    this.logger = logger;
    this.slaveID = slaveID;
    this.tripleStore = tripleStore;
    this.connection = connection;
    this.workingDir = workingDir;
    this.messageNotifier = messageNotifier;
    if (workingDir.exists()) {
      deleteContent(workingDir);
    } else {
      if (!workingDir.mkdirs()) {
        throw new RuntimeException(
                "The working directory " + workingDir.getAbsolutePath() + " could not be created!");
      }
    }
    receiver = new FileReceiver(workingDir, slaveID, numberOfSlaves, connection, 1,
            new String[] { "enc.gz" }, logger);
    receivedGraphChunks = new HashSet<>();
  }

  @Override
  public void processMessage(byte[][] message) {
    if (message == null || message.length == 0) {
      return;
    }
    try {
      MessageType mType = null;
      try {
        mType = MessageType.valueOf(message[0][0]);
        switch (mType) {
          case START_FILE_TRANSFER:
            long totalNumberOfChunks = NumberConversion.bytes2long(message[1]);
            if (totalNumberOfChunks < receiver.getMaximalNumberOfParallelRequests()) {
              receiver.adjustMaximalNumberOfParallelRequests((int) totalNumberOfChunks);
            }
            receiver.requestFiles();
            break;
          case FILE_CHUNK_RESPONSE:
            int fileID = NumberConversion.bytes2int(message[1]);
            long chunkID = NumberConversion.bytes2long(message[2]);
            totalNumberOfChunks = NumberConversion.bytes2long(message[3]);
            byte[] chunkContent = message[4];
            try {
              receiver.receiveFileChunk(fileID, chunkID, totalNumberOfChunks, chunkContent);
              File graphChunk = receiver.getFileWithID(fileID);
              if (!receivedGraphChunks.contains(graphChunk)) {
                receivedGraphChunks.add(graphChunk);
              }
              if (receiver.isFinished()) {
                receiver.close();
                start();
              }
            } catch (IOException e) {
              if (logger != null) {
                logger.finer("error during receiving a graph chunk");
                logger.throwing(e.getStackTrace()[0].getClassName(),
                        e.getStackTrace()[0].getMethodName(), e);
              }
              connection.sendFailNotification(slaveID, e.getMessage());
              close();
            }
            break;
          default:
            if (logger != null) {
              logger.finer("Unsupported message type: " + mType.name());
            }
        }
      } catch (IllegalArgumentException e) {
        if (logger != null) {
          logger.finer("Unknown message type: " + message[0][0]);
          logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
                  e);
        }
      } catch (BufferUnderflowException | IndexOutOfBoundsException e) {
        if (logger != null) {
          logger.finer("Message of type " + mType + " is too short.");
          logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
                  e);
        }
      }
    } catch (RuntimeException e) {
      if (logger != null) {
        logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
                e);
      }
      connection.sendFailNotification(slaveID, e.getMessage());
      close();
    }
  }

  @Override
  public void processMessage(byte[] message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void run() {
    try {
      for (File graphChunk : receivedGraphChunks) {
        if (graphChunk.exists()) {
          tripleStore.storeTriples(graphChunk);
        }
        if (isInterrupted()) {
          break;
        }
      }
    } catch (RuntimeException e) {
      if (logger != null) {
        logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
                e);
      }
      connection.sendFailNotification(slaveID, e.getMessage());
      close();
    }

    if (!isInterrupted()) {
      connection.sendFinish(slaveID);
    }
    close();
  }

  @Override
  public int getSlaveID() {
    return slaveID;
  }

  @Override
  public void close() {
    messageNotifier.unregisterMessageListener(GraphChunkListener.class, this);
    receiver.close();
    deleteContent(workingDir);
    workingDir.delete();
  }

  private void deleteContent(File workingDir) {
    if (workingDir.exists()) {
      for (File containedFile : workingDir.listFiles()) {
        containedFile.delete();
      }
    }
  }

}
