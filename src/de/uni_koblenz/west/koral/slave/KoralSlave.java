package de.uni_koblenz.west.koral.slave;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.executor.messagePassing.MessageReceiverListener;
import de.uni_koblenz.west.koral.common.messages.MessageType;
import de.uni_koblenz.west.koral.common.system.ConfigurationException;
import de.uni_koblenz.west.koral.common.system.KoralSystem;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.slave.networkManager.SlaveNetworkManager;
import de.uni_koblenz.west.koral.slave.triple_store.TripleStoreAccessor;
import de.uni_koblenz.west.koral.slave.triple_store.loader.GraphChunkListener;
import de.uni_koblenz.west.koral.slave.triple_store.loader.impl.GraphChunkLoader;

import java.io.File;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;

/**
 * The implementation of the Koral slaves.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class KoralSlave extends KoralSystem {

  private final File tmpDir;

  private TripleStoreAccessor tripleStore;

  public KoralSlave(Configuration conf) throws ConfigurationException {
    super(conf, KoralSlave.getCurrentIP(conf),
            new SlaveNetworkManager(conf, KoralSlave.getCurrentIP(conf)));
    try {
      tmpDir = new File(conf.getTmpDir());
      if (!tmpDir.exists()) {
        tmpDir.mkdirs();
      }
      setTripleStore(new TripleStoreAccessor(conf, logger));
    } catch (Throwable t) {
      if (logger != null) {
        logger.throwing(t.getStackTrace()[0].getClassName(), t.getStackTrace()[0].getMethodName(),
                t);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
      }
      throw t;
    }
  }

  @Override
  protected void setTripleStore(TripleStoreAccessor tripleStore) {
    this.tripleStore = tripleStore;
    super.setTripleStore(tripleStore);
  }

  private static String[] getCurrentIP(Configuration conf) throws ConfigurationException {
    for (int i = 0; i < conf.getNumberOfSlaves(); i++) {
      String[] slave = conf.getSlave(i);
      try {
        NetworkInterface ni = NetworkInterface.getByInetAddress(InetAddress.getByName(slave[0]));
        if (ni != null) {
          return slave;
        }
      } catch (SocketException | UnknownHostException e) {
      }
    }
    throw new ConfigurationException(
            "The current slave cannot be found in the configuration file.");
  }

  @Override
  public void runOneIteration() {
    byte[] receive = getNetworkManager().receive();
    if (receive != null) {
      processMessage(receive);
      if (!isInterrupted()) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
        }
      }
    }
  }

  private void processMessage(byte[] receivedMessage) {
    if ((receivedMessage == null) || (receivedMessage.length == 0)) {
      return;
    }
    try {
      MessageType messageType = null;
      try {
        messageType = MessageType.valueOf(receivedMessage[0]);
        int slaveID = getNetworkManager().getCurrentID();
        switch (messageType) {
          case CLEAR:
            clear();
            break;
          case START_FILE_TRANSFER:
            byte[][] message = new byte[3][];
            message[0] = new byte[] { receivedMessage[0] };
            message[1] = getNetworkManager().receive(true);
            message[2] = getNetworkManager().receive(true);
            File workingDir = new File(
                    tmpDir.getAbsolutePath() + File.separatorChar + "graphLoader" + slaveID);
            GraphChunkListener loader = new GraphChunkLoader(slaveID,
                    getNetworkManager().getNumberOfSlaves(), workingDir,
                    (SlaveNetworkManager) getNetworkManager(), tripleStore, this, logger);
            registerMessageListener(GraphChunkListener.class, loader);
            notifyMessageListener(messageType.getListenerType(), slaveID, message);
            break;
          case QUERY_CREATE:
            getWorkerManager().createQuery(receivedMessage);
            break;
          case QUERY_START:
            getWorkerManager().startQuery(receivedMessage);
            break;
          case QUERY_ABORTION:
            getWorkerManager().abortQuery(receivedMessage);
            break;
          case QUERY_MAPPING_BATCH:
          case QUERY_TASK_FINISHED:
            short senderID = NumberConversion.bytes2short(receivedMessage, 1);
            notifyMessageListener(MessageReceiverListener.class, senderID, receivedMessage);
            break;
          default:
            if (logger != null) {
              logger.finer("Unknown message type received from slave: " + messageType.name());
            }
        }
      } catch (IllegalArgumentException e) {
        if (logger != null) {
          logger.finer("Unknown message type: " + receivedMessage[0]);
          logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
                  e);
        }
      } catch (BufferUnderflowException | IndexOutOfBoundsException e) {
        if (logger != null) {
          logger.finer("Message of type " + messageType + " is too short with only "
                  + receivedMessage.length + " received bytes.");
          logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
                  e);
        }
      }
    } catch (RuntimeException e) {
      if (logger != null) {
        logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
                e);
      }
    }
  }

  @Override
  public void shutDown() {
    super.shutDown();
    tripleStore.close();
  }

  @Override
  public void clear() {
    super.clear();
    tripleStore.clear();
    if (logger != null) {
      logger.info("slave " + getNetworkManager().getCurrentID() + " cleared.");
    }
  }

  public static void main(String[] args) {
    String className = KoralSlave.class.getName();
    String additionalArgs = "";
    Options options = KoralSystem.createCommandLineOptions();
    try {
      CommandLine line = KoralSystem.parseCommandLineArgs(options, args);
      Configuration conf = KoralSystem.initializeConfiguration(options, line, className,
              additionalArgs);

      KoralSlave slave;
      try {
        slave = new KoralSlave(conf);
        slave.start();
      } catch (ConfigurationException e) {
        e.printStackTrace();
      }

    } catch (ParseException e) {
      e.printStackTrace();
      KoralSystem.printUsage(className, options, additionalArgs);
    }
  }

}
