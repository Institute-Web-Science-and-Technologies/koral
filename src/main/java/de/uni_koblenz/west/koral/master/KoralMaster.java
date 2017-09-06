/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License along with Koral. If not,
 * see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
package de.uni_koblenz.west.koral.master;

import java.nio.BufferUnderflowException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.executor.WorkerTask;
import de.uni_koblenz.west.koral.common.executor.messagePassing.MessageReceiverListener;
import de.uni_koblenz.west.koral.common.messages.MessageType;
import de.uni_koblenz.west.koral.common.system.KoralSystem;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.client_manager.ClientConnectionManager;
import de.uni_koblenz.west.koral.master.client_manager.ClientMessageProcessor;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.koral.master.networkManager.MasterNetworkManager;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;

/**
 * The Koral master implementation.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class KoralMaster extends KoralSystem {

  private final ClientMessageProcessor clientMessageProcessor;

  private final DictionaryEncoder dictionary;

  private final GraphStatistics statistics;

  private boolean graphHasBeenLoaded;

  public KoralMaster(Configuration conf) {
    this(conf, true);
  }

  public KoralMaster(Configuration conf, boolean contactSlaves) {
    super(conf, conf.getMaster(), new MasterNetworkManager(conf, conf.getMaster(), contactSlaves),
        true);
    try {
      ClientConnectionManager clientConnections = new ClientConnectionManager(conf, logger);
      dictionary = new DictionaryEncoder(conf, logger, measurementCollector);
      statistics = new GraphStatistics(conf, (short) conf.getNumberOfSlaves(), logger);
      clientMessageProcessor = new ClientMessageProcessor(conf, clientConnections, this,
          contactSlaves, logger, measurementCollector);
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
      try {
        processMessage(receive);
      } catch (Exception e) {
        if (logger != null) {
          logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
              e);
        }
      }
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
        case GRAPH_LOADING_FAILED:
          short senderID = NumberConversion.bytes2short(receivedMessage, 1);
          notifyMessageListener(messageType.getListenerType(), senderID, receivedMessage);
          break;
        case QUERY_CREATED:
        case QUERY_MAPPING_BATCH:
        case QUERY_TASK_FINISHED:
        case QUERY_TASK_FAILED:
          senderID = NumberConversion.bytes2short(receivedMessage, 1);
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
    getNetworkManager().sendToAllSlaves(new byte[] {MessageType.CLEAR.getValue()});
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
    String className = KoralMaster.class.getName();
    String additionalArgs = "";
    Options options = KoralMaster.createCommandLineOptions();
    try {
      CommandLine line = KoralSystem.parseCommandLineArgs(options, args);
      if (line == null) {
        return;
      }
      Configuration conf =
          KoralSystem.initializeConfiguration(options, line, className, additionalArgs);

      KoralMaster master = new KoralMaster(conf, !line.hasOption('o'));
      master.start();

    } catch (ParseException e) {
      e.printStackTrace();
      KoralSystem.printUsage(className, options, additionalArgs);
    }
  }

  protected static Options createCommandLineOptions() {
    Options options = KoralSystem.createCommandLineOptions();
    Option coverOnly = Option.builder("o").longOpt("coverOnly")
        .desc("if set, the graph cover is created but the chunks are not transmitted to the slaves")
        .required(false).build();
    options.addOption(coverOnly);
    return options;
  }

}
