package de.uni_koblenz.west.cidre.master;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.executor.WorkerTask;
import de.uni_koblenz.west.cidre.common.executor.messagePassing.MessageReceiverListener;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.system.CidreSystem;
import de.uni_koblenz.west.cidre.common.utils.NumberConversion;
import de.uni_koblenz.west.cidre.master.client_manager.ClientConnectionManager;
import de.uni_koblenz.west.cidre.master.client_manager.ClientMessageProcessor;
import de.uni_koblenz.west.cidre.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.cidre.master.networkManager.MasterNetworkManager;
import de.uni_koblenz.west.cidre.master.statisticsDB.GraphStatistics;

import java.nio.BufferUnderflowException;
import java.util.Arrays;

/**
 * The CIDRE master implementation.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class CidreMaster extends CidreSystem {

  private final ClientMessageProcessor clientMessageProcessor;

  private final DictionaryEncoder dictionary;

  private final GraphStatistics statistics;

  private boolean graphHasBeenLoaded;

  public CidreMaster(Configuration conf) {
    super(conf, conf.getMaster(), new MasterNetworkManager(conf, conf.getMaster()));
    try {
      ClientConnectionManager clientConnections = new ClientConnectionManager(conf, logger);
      dictionary = new DictionaryEncoder(conf, logger);
      statistics = new GraphStatistics(conf, (short) conf.getNumberOfSlaves(), logger);
      clientMessageProcessor = new ClientMessageProcessor(conf, clientConnections, this, logger);
      graphHasBeenLoaded = !dictionary.isEmpty();
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

  public DictionaryEncoder getDictionary() {
    return dictionary;
  }

  public GraphStatistics getStatistics() {
    return statistics;
  }

  public void executeTask(WorkerTask rootTask) {
    getWorkerManager().addTask(rootTask);
  }

  @Override
  public void runOneIteration() {
    boolean messageReceived = false;
    // process client message
    messageReceived = clientMessageProcessor.processMessage(graphHasBeenLoaded);
    graphHasBeenLoaded = clientMessageProcessor.isGraphLoaded(graphHasBeenLoaded);
    byte[] receive = getNetworkManager().receive();
    if (receive != null) {
      messageReceived = true;
      processMessage(receive);
    }
    if (!isInterrupted() && !messageReceived) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
    }
  }

  private void processMessage(byte[] receivedMessage) {
    if ((receivedMessage == null) || (receivedMessage.length == 0)) {
      return;
    }
    MessageType messageType = null;
    try {
      messageType = MessageType.valueOf(receivedMessage[0]);
      switch (messageType) {
        case GRAPH_LOADING_COMPLETE:
          byte[][] message = new byte[2][];
          message[0] = new byte[] { receivedMessage[0] };
          message[1] = new byte[2];
          System.arraycopy(receivedMessage, 1, message[1], 0, message[1].length);
          short slaveID = NumberConversion.bytes2short(message[1]);
          notifyMessageListener(messageType.getListenerType(), slaveID, message);
          break;
        case GRAPH_LOADING_FAILED:
          message = new byte[3][];
          message[0] = new byte[] { receivedMessage[0] };
          message[1] = Arrays.copyOfRange(receivedMessage, 1, 3);
          message[2] = Arrays.copyOfRange(receivedMessage, 3, receivedMessage.length);
          slaveID = NumberConversion.bytes2short(message[1]);
          notifyMessageListener(messageType.getListenerType(), slaveID, message);
          break;
        case QUERY_CREATED:
        case QUERY_MAPPING_BATCH:
        case QUERY_TASK_FINISHED:
        case QUERY_TASK_FAILED:
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
  }

  public short getComputerId() {
    return (short) getNetworkManager().getCurrentID();
  }

  public int getNumberOfSlaves() {
    return getNetworkManager().getNumberOfSlaves();
  }

  @Override
  public void clear() {
    super.clear();
    getNetworkManager().sendToAllSlaves(new byte[] { MessageType.CLEAR.getValue() });
    clientMessageProcessor.clear();
    dictionary.clear();
    statistics.clear();
    graphHasBeenLoaded = false;
    if (logger != null) {
      logger.info("master cleared");
    }
  }

  @Override
  public void shutDown() {
    super.shutDown();
    clientMessageProcessor.close();
    dictionary.close();
    statistics.close();
  }

  public static void main(String[] args) {
    String className = CidreMaster.class.getName();
    String additionalArgs = "";
    Options options = CidreSystem.createCommandLineOptions();
    try {
      CommandLine line = CidreSystem.parseCommandLineArgs(options, args);
      Configuration conf = CidreSystem.initializeConfiguration(options, line, className,
              additionalArgs);

      CidreMaster master = new CidreMaster(conf);
      master.start();

    } catch (ParseException e) {
      e.printStackTrace();
      CidreSystem.printUsage(className, options, additionalArgs);
    }
  }

}
