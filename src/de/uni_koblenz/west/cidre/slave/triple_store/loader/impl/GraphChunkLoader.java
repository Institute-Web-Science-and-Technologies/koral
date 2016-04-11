package de.uni_koblenz.west.cidre.slave.triple_store.loader.impl;

import de.uni_koblenz.west.cidre.common.ftp.FTPClient;
import de.uni_koblenz.west.cidre.common.messages.MessageNotifier;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.master.CidreMaster;
import de.uni_koblenz.west.cidre.slave.networkManager.SlaveNetworkManager;
import de.uni_koblenz.west.cidre.slave.triple_store.TripleStoreAccessor;
import de.uni_koblenz.west.cidre.slave.triple_store.loader.GraphChunkListener;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.BufferUnderflowException;
import java.util.logging.Logger;
import java.util.regex.Pattern;

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

  private final SlaveNetworkManager connection;

  private final TripleStoreAccessor tripleStore;

  private final MessageNotifier messageNotifier;

  private String[] ftpServer;

  private String remoteGraphChunkFileName;

  public GraphChunkLoader(int slaveID, int numberOfSlaves, File workingDir,
          SlaveNetworkManager networkManager, TripleStoreAccessor tripleStore,
          MessageNotifier messageNotifier, Logger logger) {
    this.logger = logger;
    this.slaveID = slaveID;
    connection = networkManager;
    this.tripleStore = tripleStore;
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
  }

  @Override
  public void processMessage(byte[][] message) {
    if ((message == null) || (message.length == 0)) {
      return;
    }
    try {
      MessageType mType = null;
      try {
        mType = MessageType.valueOf(message[0][0]);
        switch (mType) {
          case START_FILE_TRANSFER:
            ftpServer = new String(message[1], "UTF-8").split(Pattern.quote(":"));
            remoteGraphChunkFileName = new String(message[2], "UTF-8");
            start();
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
    } catch (UnsupportedEncodingException | RuntimeException e) {
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
      File graphChunk = new File(
              workingDir.getAbsolutePath() + File.separator + remoteGraphChunkFileName);
      FTPClient ftpClient = new FTPClient(logger);
      ftpClient.downloadFile(remoteGraphChunkFileName, graphChunk, ftpServer[0], ftpServer[1]);
      if (graphChunk.exists()) {
        tripleStore.storeTriples(graphChunk);
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
