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
package de.uni_koblenz.west.koral.master.tasks;

import de.uni_koblenz.west.koral.common.ftp.FTPServer;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.measurement.MeasurementType;
import de.uni_koblenz.west.koral.common.messages.MessageNotifier;
import de.uni_koblenz.west.koral.common.messages.MessageType;
import de.uni_koblenz.west.koral.common.messages.MessageUtils;
import de.uni_koblenz.west.koral.common.networManager.NetworkManager;
import de.uni_koblenz.west.koral.common.utils.GraphFileFilter;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.client_manager.ClientConnectionManager;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.koral.master.graph_cover_creator.CoverStrategyType;
import de.uni_koblenz.west.koral.master.graph_cover_creator.GraphCoverCreator;
import de.uni_koblenz.west.koral.master.graph_cover_creator.GraphCoverCreatorFactory;
import de.uni_koblenz.west.koral.master.graph_cover_creator.NHopReplicator;
import de.uni_koblenz.west.koral.master.graph_cover_creator.impl.MoleculeHashCoverCreator;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.koral.slave.KoralSlave;

import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Thread that is used to load a graph, i.e.,
 * <ol>
 * <li>Requesting graph files from client.</li>
 * <li>Creating the requested graph cover.</li>
 * <li>Encoding the graph chunks and collecting statistical information.</li>
 * <li>Sending encoded graph chunks to the {@link KoralSlave}s.</li>
 * <li>Waiting for loading finished messages off all {@link KoralSlave}s.</li>
 * </ol>
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class GraphLoaderTask extends Thread implements Closeable {

  private final Logger logger;

  private final MeasurementCollector measurementCollector;

  private final int clientId;

  private final ClientConnectionManager clientConnections;

  private final NetworkManager slaveConnections;

  private final DictionaryEncoder dictionary;

  private final GraphStatistics statistics;

  private final File workingDir;

  private final File graphFilesDir;

  private GraphCoverCreator coverCreator;

  private int replicationPathLength;

  private int numberOfGraphChunks;

  private ClientConnectionKeepAliveTask keepAliveThread;

  private boolean graphIsLoadingOrLoaded;

  private final MessageNotifier messageNotifier;

  private final String externalFtpIpAddress;

  private final String internalFtpIpAddress;

  private final String ftpPort;

  private final FTPServer ftpServer;

  private volatile int numberOfBusySlaves;

  private boolean isStarted;

  private LoadingState state;

  private final boolean contactSlaves;

  public GraphLoaderTask(int clientID, ClientConnectionManager clientConnections,
          NetworkManager slaveConnections, String externalFtpIpAddress, String internalFtpIpAddress,
          String ftpPort, DictionaryEncoder dictionary, GraphStatistics statistics, File tmpDir,
          MessageNotifier messageNotifier, Logger logger, MeasurementCollector collector,
          boolean contactSlaves) {
    setDaemon(true);
    graphIsLoadingOrLoaded = true;
    this.contactSlaves = contactSlaves;
    isStarted = false;
    clientId = clientID;
    this.clientConnections = clientConnections;
    this.slaveConnections = slaveConnections;
    this.dictionary = dictionary;
    this.statistics = statistics;
    this.messageNotifier = messageNotifier;
    this.logger = logger;
    measurementCollector = collector;
    this.externalFtpIpAddress = externalFtpIpAddress;
    this.internalFtpIpAddress = internalFtpIpAddress;
    this.ftpPort = ftpPort;
    ftpServer = new FTPServer();
    workingDir = new File(
            tmpDir.getAbsolutePath() + File.separatorChar + "koral_client_" + clientId);
    if (workingDir.exists()) {
      loadState();
      if (state == LoadingState.START) {
        deleteContent(workingDir);
      }
    } else {
      if (!workingDir.mkdirs()) {
        throw new RuntimeException(
                "The working directory " + workingDir.getAbsolutePath() + " could not be created!");
      }
      setState(LoadingState.START);
    }
    graphFilesDir = new File(workingDir.getAbsolutePath() + File.separatorChar + "graphFiles");
  }

  private void setState(LoadingState state) {
    try (FileWriter fw = new FileWriter(
            workingDir.getAbsolutePath() + File.separator + "GraphLoaderTaskState.txt");) {
      fw.write(state.toString());
      this.state = state;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void loadState() {
    File stateFile = new File(
            workingDir.getAbsolutePath() + File.separator + "GraphLoaderTaskState.txt");
    if (stateFile.exists()) {
      try (LineNumberReader reader = new LineNumberReader(new FileReader(stateFile));) {
        String state = reader.readLine();
        this.state = LoadingState.valueOf(state);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      state = LoadingState.START;
    }
  }

  private void deleteContent(File dir) {
    if (dir.exists()) {
      for (File file : dir.listFiles()) {
        if (file.isDirectory()) {
          deleteContent(file);
        }
        if (!file.delete()) {
          throw new RuntimeException(file.getAbsolutePath() + " could not be deleted!");
        }
      }
    }
  }

  public void loadGraph(byte[][] args, int numberOfGraphChunks) {
    if (args.length < 4) {
      throw new IllegalArgumentException(
              "Loading a graph requires at least 4 arguments, but received only " + args.length
                      + " arguments.");
    }
    CoverStrategyType coverStrategy = CoverStrategyType.values()[NumberConversion
            .bytes2int(args[0])];
    int replicationPathLength = NumberConversion.bytes2int(args[1]);
    int numberOfFiles = NumberConversion.bytes2int(args[3]);
    loadGraph(coverStrategy, replicationPathLength, numberOfGraphChunks, numberOfFiles,
            getFileExtensions(args, 4), NumberConversion.bytes2int(args[2]));
  }

  private String[] getFileExtensions(byte[][] args, int startIndex) {
    String[] fileExtension = new String[args.length - startIndex];
    for (int i = 0; i < fileExtension.length; i++) {
      fileExtension[i] = MessageUtils.convertToString(args[startIndex + i], logger);
    }
    return fileExtension;
  }

  public void loadGraph(CoverStrategyType coverStrategy, int replicationPathLength,
          int numberOfGraphChunks, int numberOfFiles, String[] fileExtensions,
          int maxMoleculeDiameter) {
    if (logger != null) {
      logger.finer("loadGraph(coverStrategy=" + coverStrategy.name() + ", replicationPathLength="
              + replicationPathLength + ", numberOfFiles=" + numberOfFiles + ")");
    }
    if ((state == LoadingState.START) && (measurementCollector != null)) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_START,
              System.currentTimeMillis(), coverStrategy.toString(),
              Integer.valueOf(replicationPathLength).toString(),
              Integer.valueOf(numberOfGraphChunks).toString());
    }
    coverCreator = GraphCoverCreatorFactory.getGraphCoverCreator(coverStrategy, logger,
            measurementCollector);
    if (coverCreator instanceof MoleculeHashCoverCreator) {
      ((MoleculeHashCoverCreator) coverCreator).setMaxMoleculeDiameter(maxMoleculeDiameter);
    }
    this.replicationPathLength = replicationPathLength;
    this.numberOfGraphChunks = numberOfGraphChunks;
    if (state == LoadingState.START) {
      ftpServer.start(externalFtpIpAddress, ftpPort, graphFilesDir, 1);
      clientConnections.send(clientId, MessageUtils.createStringMessage(
              MessageType.MASTER_SEND_FILES, externalFtpIpAddress + ":" + ftpPort, logger));
      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_FILE_TRANSFER_TO_MASTER_START,
                System.currentTimeMillis());
      }
    } else {
      receiveFilesSent();
    }
  }

  public void receiveFilesSent() {
    if (state == LoadingState.START) {
      ftpServer.close();
      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_FILE_TRANSFER_TO_MASTER_END,
                System.currentTimeMillis());
      }
    }
    start();
  }

  @Override
  public void run() {
    isStarted = true;
    try {
      keepAliveThread = new ClientConnectionKeepAliveTask(clientConnections, clientId);
      keepAliveThread.start();

      File encodedGraphFile = encodeGraphFilesInitially();
      File[] chunks = createGraphChunks(encodedGraphFile);
      File[] encodedFiles = encodeGraphChunks(chunks,
              replicationPathLength != 0 ? EncodingFileFormat.EEE
                      : coverCreator.getRequiredInputEncoding());
      collectStatistis(encodedFiles);
      encodedFiles = adjustOwnership(encodedFiles);

      if (state != LoadingState.FINISHED) {
        setState(LoadingState.TRANSMITTING);
        if (contactSlaves) {
          ftpServer.start(internalFtpIpAddress, ftpPort, workingDir, numberOfGraphChunks);
          numberOfBusySlaves = 0;
          List<GraphLoaderListener> listeners = new ArrayList<>();
          for (int i = 0; i < encodedFiles.length; i++) {
            File file = encodedFiles[i];
            if (file == null) {
              continue;
            }
            numberOfBusySlaves++;
            // slave ids start with 1!
            GraphLoaderListener listener = new GraphLoaderListener(this, i + 1);
            listeners.add(listener);
            messageNotifier.registerMessageListener(GraphLoaderListener.class, listener);
            slaveConnections.sendMore(i + 1,
                    new byte[] { MessageType.START_FILE_TRANSFER.getValue() });
            slaveConnections.sendMore(i + 1,
                    (internalFtpIpAddress + ":" + ftpPort).getBytes("UTF-8"));
            slaveConnections.send(i + 1, file.getName().getBytes("UTF-8"));
          }

          while (!isInterrupted() && (numberOfBusySlaves > 0)) {
            long currentTime = System.currentTimeMillis();
            long timeToSleep = 100 - (System.currentTimeMillis() - currentTime);
            if (!isInterrupted() && (timeToSleep > 0)) {
              try {
                Thread.sleep(timeToSleep);
              } catch (InterruptedException e) {
                break;
              }
            }
          }

          for (GraphLoaderListener listener : listeners) {
            messageNotifier.unregisterMessageListener(GraphLoaderListener.class, listener);
          }
        }
      }

      keepAliveThread.interrupt();

      if (numberOfBusySlaves == 0) {
        clientConnections.send(clientId,
                new byte[] { MessageType.CLIENT_COMMAND_SUCCEEDED.getValue() });
        if (contactSlaves) {
          setState(LoadingState.FINISHED);
          cleanWorkingDirs();
        }
      } else {
        clientConnections.send(clientId,
                MessageUtils.createStringMessage(MessageType.CLIENT_COMMAND_FAILED,
                        "Loading of graph was interrupted before all slaves have loaded the graph.",
                        logger));
      }
    } catch (Throwable e) {
      e.printStackTrace(System.out);
      if (logger != null) {
        logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
                e);
      }
      clientConnections.send(clientId,
              MessageUtils.createStringMessage(MessageType.CLIENT_COMMAND_FAILED,
                      e.getClass().getName() + ":" + e.getMessage(), logger));
    }
  }

  public void processSlaveResponse(byte[] message) {
    MessageType messageType = MessageType.valueOf(message[0]);
    if (messageType == null) {
      if (logger != null) {
        logger.finest("Ignoring message with unknown type.");
      }
    }
    switch (messageType) {
      case GRAPH_LOADING_COMPLETE:
        numberOfBusySlaves--;
        break;
      case GRAPH_LOADING_FAILED:
        // clearDatabase();
        String errorMessage = null;
        try {
          errorMessage = new String(message, Byte.BYTES + Short.BYTES,
                  message.length - Byte.BYTES - Short.BYTES, "UTF-8");
        } catch (UnsupportedEncodingException e) {
          errorMessage = e.getMessage();
        }
        if (logger != null) {
          logger.finer("Loading of graph failed on slave "
                  + NumberConversion.bytes2short(message, 1) + ". Reason: " + errorMessage);
        }
        clientConnections.send(clientId, MessageUtils.createStringMessage(
                MessageType.CLIENT_COMMAND_FAILED, "Loading of graph failed on slave "
                        + NumberConversion.bytes2short(message, 1) + ". Reason: " + errorMessage,
                logger));
        close();
        break;
      default:
        if (logger != null) {
          logger.finest("Ignoring message of type " + messageType);
        }
    }
  }

  private File encodeGraphFilesInitially() {
    File encodedFiles = null;
    if ((state == LoadingState.START) || (state == LoadingState.INITIAL_ENCODING)) {
      setState(LoadingState.INITIAL_ENCODING);
      if (logger != null) {
        logger.finer("initial encoding of graph chunks");
      }
      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_INITIAL_ENCODING_START,
                System.currentTimeMillis());
      }
      clientConnections.send(clientId, MessageUtils.createStringMessage(
              MessageType.MASTER_WORK_IN_PROGRESS, "Started initial encoding of graph.", logger));

      File[] graphFiles = graphFilesDir.isDirectory()
              ? graphFilesDir.listFiles(new GraphFileFilter())
              : new File[] { graphFilesDir };
      encodedFiles = dictionary.encodeOriginalGraphFiles(graphFiles, workingDir,
              coverCreator.getRequiredInputEncoding(), numberOfGraphChunks);

      for (File file : graphFiles) {
        if (file != null) {
          file.delete();
        }
      }

      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_INITIAL_ENCODING_END,
                System.currentTimeMillis());
      }
      if (logger != null) {
        logger.finer("initial encoding of graph finished");
      }
      clientConnections.send(clientId,
              MessageUtils.createStringMessage(MessageType.MASTER_WORK_IN_PROGRESS,
                      "Finished initial encoding of graph chunks.", logger));
    } else {
      encodedFiles = dictionary.getSemiEncodedGraphFile(workingDir);
    }
    return encodedFiles;
  }

  private File[] createGraphChunks(File encodedGraphFile) {
    File[] chunks = null;
    if ((state == LoadingState.INITIAL_ENCODING) || (state == LoadingState.GRAPH_COVER_CREATION)) {
      setState(LoadingState.GRAPH_COVER_CREATION);
      if (logger != null) {
        logger.finer("creation of graph cover started");
      }
      clientConnections.send(clientId, MessageUtils.createStringMessage(
              MessageType.MASTER_WORK_IN_PROGRESS, "Started creation of graph cover.", logger));

      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_COVER_CREATION_START,
                System.currentTimeMillis());
      }
      chunks = coverCreator.createGraphCover(dictionary, encodedGraphFile, workingDir,
              numberOfGraphChunks);
      encodedGraphFile.delete();
      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_COVER_CREATION_END,
                System.currentTimeMillis());
      }
    } else {
      chunks = coverCreator.getGraphChunkFiles(workingDir, numberOfGraphChunks);
    }

    if (replicationPathLength != 0) {
      if (state == LoadingState.GRAPH_COVER_CREATION) {
        chunks = encodeGraphChunks(chunks, coverCreator.getRequiredInputEncoding());
        setState(LoadingState.N_HOP_REPLICATION);
      } else {
        chunks = dictionary.getFullyEncodedGraphChunks(workingDir, numberOfGraphChunks);
      }
      NHopReplicator replicator = new NHopReplicator(logger, measurementCollector);
      if ((state == LoadingState.GRAPH_COVER_CREATION)
              || (state == LoadingState.N_HOP_REPLICATION)) {
        setState(LoadingState.N_HOP_REPLICATION);
        if (measurementCollector != null) {
          measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_NHOP_REPLICATION_START,
                  System.currentTimeMillis());
        }
        chunks = replicator.createNHopReplication(chunks, workingDir, replicationPathLength);
        if (measurementCollector != null) {
          measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_NHOP_REPLICATION_END,
                  System.currentTimeMillis());
        }
      } else {
        chunks = replicator.getGraphChunkFiles(workingDir, numberOfGraphChunks);
      }
    }

    if ((state == LoadingState.GRAPH_COVER_CREATION) || (state == LoadingState.N_HOP_REPLICATION)) {
      if (logger != null) {
        logger.finer("creation of graph cover finished");
      }
      clientConnections.send(clientId, MessageUtils.createStringMessage(
              MessageType.MASTER_WORK_IN_PROGRESS, "Finished creation of graph cover.", logger));
    }
    return chunks;
  }

  private File[] encodeGraphChunks(File[] plainGraphChunks,
          EncodingFileFormat inputEncodingFormat) {
    File[] encodedFiles = null;
    if ((state == LoadingState.GRAPH_COVER_CREATION) || (state == LoadingState.N_HOP_REPLICATION)
            || (state == LoadingState.FINAL_ENCODING)) {
      setState(LoadingState.FINAL_ENCODING);
      if (logger != null) {
        logger.finer("final encoding of graph chunks");
      }
      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_FINAL_ENCODING_START,
                System.currentTimeMillis());
      }
      clientConnections.send(clientId,
              MessageUtils.createStringMessage(MessageType.MASTER_WORK_IN_PROGRESS,
                      "Started final encoding of graph chunks.", logger));

      encodedFiles = dictionary.encodeGraphChunksCompletely(plainGraphChunks, workingDir,
              inputEncodingFormat);

      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_FINAL_ENCODING_END,
                System.currentTimeMillis());
      }
      if (logger != null) {
        logger.finer("final encoding of graph chunks finished");
      }
      clientConnections.send(clientId,
              MessageUtils.createStringMessage(MessageType.MASTER_WORK_IN_PROGRESS,
                      "Finished final encoding of graph chunks.", logger));
    } else {
      encodedFiles = dictionary.getFullyEncodedGraphChunks(workingDir, numberOfGraphChunks);
    }
    return encodedFiles;
  }

  private void collectStatistis(File[] encodedChunks) {
    if ((state == LoadingState.FINAL_ENCODING) || (state == LoadingState.STATISTIC_COLLECTION)) {
      setState(LoadingState.STATISTIC_COLLECTION);
      if (logger != null) {
        logger.finer("collecting statistics");
      }
      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_COLLECTING_STATISTICS_START,
                System.currentTimeMillis());
      }
      clientConnections.send(clientId, MessageUtils.createStringMessage(
              MessageType.MASTER_WORK_IN_PROGRESS, "Started collecting statistics.", logger));

      statistics.collectStatistics(encodedChunks);

      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_COLLECTING_STATISTICS_END,
                System.currentTimeMillis());
      }
      if (logger != null) {
        logger.finer("collecting statistics finished");
      }
      clientConnections.send(clientId, MessageUtils.createStringMessage(
              MessageType.MASTER_WORK_IN_PROGRESS, "Finished collecting statistics.", logger));
    }
  }

  private File[] adjustOwnership(File[] encodedChunks) {
    if ((state == LoadingState.STATISTIC_COLLECTION) || (state == LoadingState.SETTING_OWNERSHIP)) {
      setState(LoadingState.SETTING_OWNERSHIP);
      if (logger != null) {
        logger.finer("adjusting ownership");
      }
      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_ADJUSTING_OWNERSHIP_START,
                System.currentTimeMillis());
      }
      clientConnections.send(clientId, MessageUtils.createStringMessage(
              MessageType.MASTER_WORK_IN_PROGRESS, "Started adjusting ownership.", logger));

      File[] result = statistics.adjustOwnership(encodedChunks, workingDir);

      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_ADJUSTING_OWNERSHIP_END,
                System.currentTimeMillis());
      }
      if (logger != null) {
        logger.finer("adjusting ownership finished");
      }
      clientConnections.send(clientId, MessageUtils.createStringMessage(
              MessageType.MASTER_WORK_IN_PROGRESS, "Finished adjusting ownership.", logger));
      return result;
    } else {
      return statistics.getAdjustedFiles(workingDir);
    }
  }

  public boolean isGraphLoadingOrLoaded() {
    return graphIsLoadingOrLoaded;
  }

  @Override
  public void close() {
    try {
      coverCreator.close();
      if (isAlive()) {
        interrupt();
        clientConnections.send(clientId,
                MessageUtils.createStringMessage(MessageType.CLIENT_COMMAND_FAILED,
                        "GraphLoaderTask has been closed before it could finish.", logger));
        graphIsLoadingOrLoaded = false;
      } else if (!isStarted) {
        graphIsLoadingOrLoaded = false;
      }
      if ((keepAliveThread != null) && keepAliveThread.isAlive()) {
        keepAliveThread.interrupt();
      }
      ftpServer.close();
      if ((state == LoadingState.START) || (state == LoadingState.FINISHED)) {
        cleanWorkingDirs();
      } else if (state == LoadingState.INITIAL_ENCODING) {
        dictionary.clear();
      } else if (state == LoadingState.STATISTIC_COLLECTION) {
        statistics.clear();
      }
    } finally {
      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_FINISHED,
                System.currentTimeMillis());
      }
    }
  }

  protected void cleanWorkingDirs() {
    deleteContent(graphFilesDir);
    graphFilesDir.delete();
    deleteContent(workingDir);
    workingDir.delete();
  }

}

enum LoadingState {
  START, GRAPH_COVER_CREATION, N_HOP_REPLICATION, INITIAL_ENCODING, FINAL_ENCODING, STATISTIC_COLLECTION, SETTING_OWNERSHIP, TRANSMITTING, FINISHED;
}
