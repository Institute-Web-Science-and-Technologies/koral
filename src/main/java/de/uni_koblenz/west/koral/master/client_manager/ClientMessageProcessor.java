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
package de.uni_koblenz.west.koral.master.client_manager;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.measurement.MeasurementType;
import de.uni_koblenz.west.koral.common.messages.MessageType;
import de.uni_koblenz.west.koral.common.messages.MessageUtils;
import de.uni_koblenz.west.koral.common.query.execution.QueryExecutionCoordinator;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.KoralMaster;
import de.uni_koblenz.west.koral.master.tasks.ClientConnectionKeepAliveTask;
import de.uni_koblenz.west.koral.master.tasks.GraphLoaderTask;

import java.io.Closeable;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Processes messages received from a client.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class ClientMessageProcessor implements Closeable, ClosedConnectionListener {

  private final Logger logger;

  private final MeasurementCollector measurementCollector;

  private final ClientConnectionManager clientConnections;

  private final KoralMaster master;

  private final File tmpDir;

  private final String[] ftpServer;

  private final String internalFtpIpAddress;

  private final Map<String, Integer> clientAddress2Id;

  private final Map<String, GraphLoaderTask> clientAddress2GraphLoaderTask;

  private final int numberOfChunks;

  // private final ReusableIDGenerator queryIdGenerator;

  private int nextQueryId;

  private final Map<String, QueryExecutionCoordinator> clientAddress2queryExecutionCoordinator;

  private final int mappingReceiverQueueSize;

  private final int emittedMappingsPerRound;

  private final MapDBStorageOptions storageType;

  private final boolean useTransactions;

  private final boolean writeAsynchronously;

  private final MapDBCacheOptions cacheType;

  private final boolean contactSlaves;

  public ClientMessageProcessor(Configuration conf, ClientConnectionManager clientConnections,
          KoralMaster master, boolean contactSlaves, Logger logger,
          MeasurementCollector measurementCollector) {
    this.logger = logger;
    this.measurementCollector = measurementCollector;
    this.clientConnections = clientConnections;
    this.master = master;
    this.contactSlaves = contactSlaves;
    ftpServer = conf.getFTPServer();
    internalFtpIpAddress = conf.getMaster()[0];
    numberOfChunks = conf.getNumberOfSlaves();
    tmpDir = new File(conf.getTmpDirByInstance(true));
    if (!tmpDir.exists() || !tmpDir.isDirectory()) {
      throw new IllegalArgumentException(
              "The temporary directory " + conf.getTmpDirByInstance(true) + " is not a directory.");
    }
    clientAddress2Id = new HashMap<>();
    clientAddress2GraphLoaderTask = new HashMap<>();
    clientAddress2queryExecutionCoordinator = new HashMap<>();
    // queryIdGenerator = new ReusableIDGenerator();
    nextQueryId = 0;
    this.clientConnections.registerClosedConnectionListener(this);
    mappingReceiverQueueSize = conf.getReceiverQueueSize();
    emittedMappingsPerRound = conf.getMaxEmittedMappingsPerRound();
    cacheType = conf.getJoinCacheType();
    storageType = conf.getJoinCacheStorageType();
    useTransactions = conf.useTransactionsForJoinCache();
    writeAsynchronously = conf.isJoinCacheAsynchronouslyWritten();
  }

  /**
   * @param graphHasBeenLoaded
   * @return <code>true</code>, iff a message was received
   */
  public boolean processMessage(boolean graphHasBeenLoaded) {
    byte[] message = clientConnections.receive(false);
    if ((message != null) && (message.length > 0)) {
      try {
        MessageType messageType = MessageType.valueOf(message[0]);
        switch (messageType) {
          case CLIENT_CONNECTION_CREATION:
            processCreateConnection(message);
            break;
          case CLIENT_IS_ALIVE:
            processKeepAlive(message);
            break;
          case CLIENT_COMMAND:
            processCommand(message, graphHasBeenLoaded);
            break;
          case CLIENT_FILES_SENT:
            processFilesSent(message);
            break;
          case CLIENT_COMMAND_ABORTED:
            processAbortCommand(message);
            break;
          case CLIENT_CLOSES_CONNECTION:
            processCloseConnection(message);
            break;
          default:
            if (logger != null) {
              logger.finest("ignoring message with unsupported message type: " + messageType);
            }
            return true;
        }
      } catch (IllegalArgumentException e) {
        if (logger != null) {
          logger.finest("ignoring message with unknown message type: " + message[0]);
          logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
                  e);
        }
        return true;
      }
    }
    return message != null;
  }

  private void processCreateConnection(byte[] message) {
    String address = MessageUtils.extractMessageString(message, logger);
    if (logger != null) {
      logger.finer("client " + address + " tries to establish a connection");
    }
    int clientID = clientConnections.createConnection(address);
    clientAddress2Id.put(address, clientID);
    clientConnections.send(clientID,
            new byte[] { MessageType.CLIENT_CONNECTION_CONFIRMATION.getValue() });
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.CLIENT_STARTS_CONNECTION,
              System.currentTimeMillis(), address, Integer.valueOf(clientID).toString());
    }
  }

  private void processKeepAlive(byte[] message) {
    String address;
    address = MessageUtils.extractMessageString(message, logger);
    Integer cID = clientAddress2Id.get(address);
    if (cID != null) {
      clientConnections.updateTimerFor(cID.intValue());
    } else if (logger != null) {
      logger.finest("ignoring keep alive from client " + address + ". Connection already closed.");
    }
  }

  private void processCommand(byte[] message, boolean graphHasBeenLoaded) {
    byte[] buffer = clientConnections.receive(true);
    if (buffer == null) {
      if (logger != null) {
        logger.finest("Client has sent a command request but missed to send his id.");
      }
      return;
    }
    String address = MessageUtils.convertToString(buffer, logger);

    buffer = clientConnections.receive(true);
    if (buffer == null) {
      if (logger != null) {
        logger.finest("Client " + address
                + " has sent a command request but did not send the actual command.");
      }
      return;
    }
    String command = MessageUtils.convertToString(buffer, logger);

    buffer = clientConnections.receive(true);
    if (buffer == null) {
      if (logger != null) {
        logger.finest("Client " + address + " has sent the command " + command
                + " but did not specify the number of arguments");
      }
      return;
    }
    int numberOfArguments = NumberConversion.bytes2int(buffer);

    byte[][] arguments = new byte[numberOfArguments][];
    for (int i = 0; i < numberOfArguments; i++) {
      buffer = clientConnections.receive(true);
      if (buffer == null) {
        if (logger != null) {
          logger.finest("Client " + address + " has sent the command " + command + " that requires "
                  + numberOfArguments + " arguments. But it has received only " + i
                  + " arguments.");
        }
        return;
      }
      arguments[i] = buffer;
    }

    if (address.trim().isEmpty()) {
      if (logger != null) {
        logger.finest("Client has not sent his address.");
      }
      return;
    }
    if (logger != null) {
      logger.finest("received command from client " + address);
    }
    Integer clientID = clientAddress2Id.get(address);
    if (clientID == null) {
      if (logger != null) {
        logger.finest("The connection to client " + address + " has already been closed.");
      }
      return;
    }

    try {
      switch (command) {
        case "load":
          // if (graphHasBeenLoaded) {
          // String errorMessage = "Loading of graph rejected: Koral is
          // currently loading a graph or it has already loaded a graph.";
          // if (logger != null) {
          // logger.finer(errorMessage);
          // }
          // clientConnections.send(clientID, MessageUtils
          // .createStringMessage(MessageType.CLIENT_COMMAND_FAILED,
          // errorMessage, logger));
          // break;
          // }
          if (measurementCollector != null) {
            measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_MESSAGE_RECEIPTION,
                    System.currentTimeMillis());
          }
          GraphLoaderTask loaderTask = new GraphLoaderTask(clientID.intValue(), clientConnections,
                  master.getNetworkManager(), ftpServer[0], internalFtpIpAddress, ftpServer[1],
                  master.getDictionary(), master.getStatistics(), tmpDir, master, logger,
                  measurementCollector, contactSlaves);
          clientAddress2GraphLoaderTask.put(address, loaderTask);
          loaderTask.loadGraph(arguments, numberOfChunks);
          break;
        case "query":
          // if (!graphHasBeenLoaded) {
          // String errorMessage = "There is no graph loaded that could be
          // queried.";
          // if (logger != null) {
          // logger.finer(errorMessage);
          // }
          // clientConnections.send(clientID, MessageUtils
          // .createStringMessage(MessageType.CLIENT_COMMAND_FAILED,
          // errorMessage, logger));
          // break;
          // }
          if (measurementCollector != null) {
            measurementCollector.measureValue(MeasurementType.QUERY_MESSAGE_RECEIPTION,
                    System.currentTimeMillis());
          }
          QueryExecutionCoordinator coordinator = new QueryExecutionCoordinator(
                  master.getComputerId(), /* queryIdGenerator.getNextId() */nextQueryId++,
                  master.getNumberOfSlaves(), mappingReceiverQueueSize, tmpDir, clientID.intValue(),
                  clientConnections, master.getDictionary(), master.getStatistics(),
                  emittedMappingsPerRound, storageType, useTransactions, writeAsynchronously,
                  cacheType, logger, measurementCollector);
          coordinator.processQueryRequest(arguments);
          clientAddress2queryExecutionCoordinator.put(address, coordinator);
          master.executeTask(coordinator);
          break;
        case "drop":
          processDropTables(clientID);
          break;
        default:
          String errorMessage = "unknown command: " + command + " with " + numberOfArguments
                  + " arguments.";
          if (logger != null) {
            logger.finer(errorMessage);
          }
          clientConnections.send(clientID, MessageUtils
                  .createStringMessage(MessageType.CLIENT_COMMAND_FAILED, errorMessage, logger));
      }
    } catch (RuntimeException e) {
      if (logger != null) {
        logger.finer("error during execution of " + command + " with " + numberOfArguments
                + " arguments.");
        logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
                e);
      }
      clientConnections.send(clientID,
              MessageUtils.createStringMessage(MessageType.CLIENT_COMMAND_FAILED,
                      "error during execution of " + command + " with " + numberOfArguments
                              + " arguments:\n" + e.getClass().getName() + ": " + e.getMessage(),
                      logger));
      // remove started graph loader tasks
      if (command.equals("load") || command.equals("query")) {
        terminateTask(address);
      }
    }
  }

  private void processDropTables(int clientID) {
    if (logger != null) {
      logger.finer("Dropping database");
    }
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.CLIENT_DROP_START,
              System.currentTimeMillis(), Integer.valueOf(clientID).toString());
    }
    Thread keepAliveThread = new ClientConnectionKeepAliveTask(clientConnections, clientID);
    keepAliveThread.start();
    master.clear();
    keepAliveThread.interrupt();
    clientConnections.send(clientID, MessageUtils.createStringMessage(
            MessageType.CLIENT_COMMAND_SUCCEEDED, "Database is dropped, successfully.", logger));
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.CLIENT_DROP_END, System.currentTimeMillis(),
              Integer.valueOf(clientID).toString());
    }
    if (logger != null) {
      logger.finer("Database is dropped.");
    }
  }

  private void processFilesSent(byte[] message) {
    String address = MessageUtils.extractMessageString(message, logger);

    if (address.trim().isEmpty()) {
      if (logger != null) {
        logger.finest("Client has not sent his address.");
      }
      return;
    }
    Integer clientID = clientAddress2Id.get(address);
    if (clientID == null) {
      if (logger != null) {
        logger.finest("The connection to client " + address + " has already been closed.");
      }
      return;
    }

    GraphLoaderTask task = clientAddress2GraphLoaderTask.get(address);
    if (task == null) {
      if (logger != null) {
        logger.finest("Client " + address
                + " has send a file chunk but there is no task that will receive the chunk.");
      }
      return;
    }
    task.receiveFilesSent();
  }

  private void processAbortCommand(byte[] message) {
    String abortionContext = MessageUtils.extractMessageString(message, logger);
    String[] parts = abortionContext.split(Pattern.quote("|"));
    if (logger != null) {
      logger.finer("client " + parts[0] + " aborts command " + parts[1]);
    }
    switch (parts[1].toLowerCase()) {
      case "load":
        terminateTask(parts[0]);
        break;
      case "query":
        terminateTask(parts[0]);
        break;
      default:
        if (logger != null) {
          logger.finer("unknown aborted command: " + parts[1]);
        }
    }
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.CLIENT_ABORTS_CONNECTION,
              System.currentTimeMillis(), parts[0], parts[1]);
    }
  }

  private void terminateTask(String address) {
    GraphLoaderTask task = clientAddress2GraphLoaderTask.get(address);
    if (task != null) {
      task.close();
      if (task.isGraphLoadingOrLoaded()) {
        // graph has loaded successfully, so it can be removed
        // otherwise let in the map so that isGraphLoaded() can notify
        // the master about aborted loading
        clientAddress2GraphLoaderTask.remove(address);
      }
    }
    QueryExecutionCoordinator query = clientAddress2queryExecutionCoordinator.get(address);
    if (query != null) {
      query.close();
      // queryIdGenerator.release(query.getQueryId());
    }
    clientAddress2GraphLoaderTask.remove(address);
  }

  private void processCloseConnection(byte[] message) {
    String address;
    Integer cID;
    address = MessageUtils.extractMessageString(message, logger);
    if (logger != null) {
      logger.finer("client " + address + " has closed connection");
    }
    terminateTask(address);
    cID = clientAddress2Id.get(address);
    if (cID != null) {
      clientConnections.closeConnection(cID.intValue());
      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.CLIENT_CLOSES_CONNECTION,
                System.currentTimeMillis(), address, Integer.valueOf(cID).toString());
      }
    } else if (logger != null) {
      logger.finest("ignoring attempt from client " + address
              + " to close the connection. Connection already closed.");
    }
  }

  @Override
  public void close() {
    stopAllQueries();
    stopAllGraphLoaderTasks();
    clientConnections.close();
  }

  public void clear() {
    stopAllQueries();
    stopAllGraphLoaderTasks();
  }

  private void stopAllQueries() {
    for (QueryExecutionCoordinator task : clientAddress2queryExecutionCoordinator.values()) {
      if (task != null) {
        task.close();
      }
    }
  }

  private void stopAllGraphLoaderTasks() {
    for (GraphLoaderTask task : clientAddress2GraphLoaderTask.values()) {
      if (task != null) {
        if (task.isAlive()) {
          task.interrupt();
        }
        task.close();
      }
    }
  }

  @Override
  public void notifyOnClosedConnection(int clientID) {
    String address = null;
    for (Entry<String, Integer> entry : clientAddress2Id.entrySet()) {
      if ((entry.getValue() != null) && (entry.getValue().intValue() == clientID)) {
        address = entry.getKey();
      }
    }
    if (address != null) {
      terminateTask(address);
      clientAddress2Id.remove(address);
      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.CLIENT_CONNECTION_TIMEOUT,
                System.currentTimeMillis(), address, Integer.valueOf(clientID).toString());
      }
    }
  }

  public boolean isGraphLoaded(boolean graphHasBeenLoaded) {
    Entry<String, GraphLoaderTask> task = null;
    for (Entry<String, GraphLoaderTask> entry : clientAddress2GraphLoaderTask.entrySet()) {
      if ((entry.getValue() != null) && (entry.getValue() instanceof GraphLoaderTask)) {
        task = entry;
        break;
      }
    }
    if (task != null) {
      if (!task.getValue().isGraphLoadingOrLoaded()) {
        clientAddress2GraphLoaderTask.remove(task.getKey());
      }
      return task.getValue().isGraphLoadingOrLoaded();
    }
    return graphHasBeenLoaded;
  }

}
