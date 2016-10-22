/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License
 * along with Koral.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
package de.uni_koblenz.west.koral.common.query.execution;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.executor.WorkerTask;
import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.measurement.MeasurementType;
import de.uni_koblenz.west.koral.common.messages.MessageType;
import de.uni_koblenz.west.koral.common.messages.MessageUtils;
import de.uni_koblenz.west.koral.common.query.Mapping;
import de.uni_koblenz.west.koral.common.query.execution.operators.SliceOperator;
import de.uni_koblenz.west.koral.common.query.parser.QueryExecutionTreeType;
import de.uni_koblenz.west.koral.common.query.parser.SparqlParser;
import de.uni_koblenz.west.koral.common.query.parser.VariableDictionary;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.client_manager.ClientConnectionManager;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.koral.master.utils.DeSerializer;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Coordinates the query execution and sends messages to the requesting client.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class QueryExecutionCoordinator extends QueryTaskBase {

  private final ClientConnectionManager clientConnections;

  private final int clientId;

  private final DictionaryEncoder dictionary;

  private final GraphStatistics statistics;

  private QueryExecutionTreeType treeType;

  private String queryString;

  private int numberOfMissingQueryCreatedMessages;

  private long lastContactWithClient;

  private SparqlParser parser;

  private final VariableDictionary varDictionary;

  private final int emittedMappingsPerRound;

  private long[] resultVariables;

  private int numberOfMissingFinishNotificationsFromSlaves;

  private final AtomicInteger numberOfUnprocessedFinishMessagesFromSlaves;

  /**
   * index of first result that is returned
   */
  private long offset;

  /**
   * number of results that are returned<br>
   * &lt;0 = return all results<br>
   * &gt;=0 = number of results to be returned
   */
  private long length;

  /*
   * Time measurements
   */

  private long querySetUpTime;

  private long queryExecutionTime;

  private long lastSentResultMappingNumber;

  public QueryExecutionCoordinator(short computerID, int queryID, int numberOfSlaves, int cacheSize,
          File cacheDir, int clientID, ClientConnectionManager clientConnections,
          DictionaryEncoder dictionary, GraphStatistics statistics, int emittedMappingsPerRound,
          MapDBStorageOptions storageType, boolean useTransactions, boolean writeAsynchronously,
          MapDBCacheOptions cacheType, Logger logger, MeasurementCollector measurementCollector) {
    super(computerID, queryID, (short) 0, numberOfSlaves, cacheSize, cacheDir);
    this.logger = logger;
    this.measurementCollector = measurementCollector;
    setEstimatedWorkLoad(Integer.MAX_VALUE);
    this.clientConnections = clientConnections;
    clientId = clientID;
    this.dictionary = dictionary;
    this.statistics = statistics;
    addInputQueue();
    numberOfMissingQueryCreatedMessages = numberOfSlaves;
    numberOfMissingFinishNotificationsFromSlaves = numberOfSlaves;
    numberOfMissingFinishedMessages += 1;
    lastContactWithClient = System.currentTimeMillis();
    varDictionary = new VariableDictionary();
    this.emittedMappingsPerRound = emittedMappingsPerRound;
    parser = new SparqlParser(dictionary, statistics, null, computerID, getQueryId(), getID(),
            numberOfSlaves, cacheSize, cacheDir, emittedMappingsPerRound, storageType,
            useTransactions, writeAsynchronously, cacheType, false);
    numberOfUnprocessedFinishMessagesFromSlaves = new AtomicInteger(0);
  }

  public void processQueryRequest(byte[][] arguments) {
    int queryTypeOctal = NumberConversion.bytes2int(arguments[0]);
    for (QueryExecutionTreeType type : QueryExecutionTreeType.values()) {
      if (type.ordinal() == queryTypeOctal) {
        treeType = type;
        break;
      }
    }
    boolean useBaseOperators = arguments[1][0] == 1;
    if (useBaseOperators) {
      parser.setUseBaseImplementation(useBaseOperators);
    }
    queryString = MessageUtils.convertToString(arguments[2], logger);
    if (logger != null) {
      logger.fine("Started query coordinator for query " + queryString.replace('\n', ' '));
    }
  }

  public int getQueryId() {
    return (int) ((getID() & 0x00_00_ff_ff_ff_ff_00_00l) >>> Short.SIZE);
  }

  @Override
  public long getCoordinatorID() {
    return getID();
  }

  @Override
  public long getCurrentTaskLoad() {
    long inputSize = getSizeOfInputQueue(0);
    return inputSize < 10 ? 10 : inputSize;
  }

  @Override
  public WorkerTask getParentTask() {
    return null;
  }

  @Override
  public boolean hasInput() {
    return true;
  }

  @Override
  public void enqueueMessage(long sender, byte[] message, int firstIndex, int messageLength) {
    MessageType mType = MessageType.valueOf(message[firstIndex]);
    switch (mType) {
      case QUERY_CREATED:
        numberOfMissingQueryCreatedMessages--;
        if (numberOfMissingQueryCreatedMessages == 0) {
          start();
          messageSender.sendQueryStart(getQueryId());
          sendMessageToClient(MessageUtils.createStringMessage(MessageType.MASTER_WORK_IN_PROGRESS,
                  "Query execution is started.", logger));
          if (logger != null) {
            logger.finer("Query " + getQueryId()
                    + " has been created on all slaves. Start of execution.");
          }
          if (measurementCollector != null) {
            measurementCollector.measureValue(MeasurementType.QUERY_COORDINATOR_SEND_QUERY_START,
                    System.currentTimeMillis(), Integer.toString(getQueryId()));
          }
          sendMessageToClient(MessageUtils.createStringMessage(MessageType.MASTER_WORK_IN_PROGRESS,
                  "Query execution tree has been created on all slaves. Start of execution.",
                  logger));
          querySetUpTime = System.currentTimeMillis() - querySetUpTime;
          queryExecutionTime = System.currentTimeMillis();
        }
        break;
      case QUERY_TASK_FAILED:
        if (logger != null) {
          logger.finer("Query " + getQueryId() + " failed.");
        }
        byte[] result = new byte[messageLength - Long.BYTES - Short.BYTES];
        result[0] = MessageType.CLIENT_COMMAND_FAILED.getValue();
        System.arraycopy(message, firstIndex + Byte.BYTES + Long.BYTES + Short.BYTES, result,
                Byte.BYTES, result.length - Byte.BYTES);
        sendMessageToClient(result);
        closeInternal();
        break;
      default:
        super.enqueueMessage(sender, message, firstIndex, messageLength);
    }
  }

  @Override
  protected void handleFinishNotification(long sender, Object object, int firstIndex,
          int messageLength) {
    synchronized (numberOfUnprocessedFinishMessagesFromSlaves) {
      numberOfUnprocessedFinishMessagesFromSlaves.incrementAndGet();
    }
  }

  @Override
  protected void handleMappingReception(long sender, byte[] message, int firstIndex, int length) {
    enqueuMessage(0, message, firstIndex, length);
  }

  @Override
  protected void executePreStartStep() {
    if (parser != null) {
      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.QUERY_COORDINATOR_START,
                System.currentTimeMillis(), Integer.toString(getQueryId()),
                queryString.replace(MeasurementCollector.columnSeparator, " ")
                        .replace(MeasurementCollector.rowSeparator, " "));
      }
      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.QUERY_COORDINATOR_PARSE_START,
                System.currentTimeMillis(), Integer.toString(getQueryId()),
                parser.isBaseImplementationUsed() ? "base" : "default", treeType.name());
      }
      querySetUpTime = System.currentTimeMillis();
      QueryOperatorBase queryExecutionTree = (QueryOperatorBase) parser.parse(queryString, treeType,
              varDictionary);
      if (queryExecutionTree instanceof SliceOperator) {
        offset = ((SliceOperator) queryExecutionTree).getOffset();
        if (offset < 0) {
          offset = 0;
        }
        length = ((SliceOperator) queryExecutionTree).getLength();
      } else {
        offset = 0;
        length = -1;
      }
      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.QUERY_COORDINATOR_PARSE_END,
                System.currentTimeMillis(), Integer.toString(getQueryId()));
        measurementCollector.measureValue(MeasurementType.QUERY_COORDINATOR_QET_NODES, getQueryId(),
                queryExecutionTree);
      }
      resultVariables = queryExecutionTree.getResultVariables();
      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.QUERY_COORDINATOR_SEND_QUERY_TO_SLAVE,
                System.currentTimeMillis(), Integer.toString(getQueryId()));
      }
      messageSender.sendQueryCreate(statistics, getQueryId(), queryExecutionTree,
              parser.isBaseImplementationUsed());
      parser = null;
    }
    sendKeepAliveMessageToClient();
  }

  @Override
  protected void executeOperationStep() {
    synchronized (numberOfUnprocessedFinishMessagesFromSlaves) {
      int messages = numberOfUnprocessedFinishMessagesFromSlaves.get();
      numberOfMissingFinishNotificationsFromSlaves -= messages;
      numberOfUnprocessedFinishMessagesFromSlaves.addAndGet(-messages);
    }
    long firstSentResultMappingNumber = lastSentResultMappingNumber + 1;
    StringBuilder result = new StringBuilder();
    int numberOfAlreadyEmittedMessages = 0;
    for (numberOfAlreadyEmittedMessages = 0; numberOfAlreadyEmittedMessages < emittedMappingsPerRound; numberOfAlreadyEmittedMessages++) {
      Mapping mapping = consumeMapping(0);
      if (mapping == null) {
        break;
      } else if (offset > 0) {
        offset--;
        numberOfAlreadyEmittedMessages--;
        continue;
      } else if ((offset <= 0) && ((length > 0) || (length < 0))) {
        lastSentResultMappingNumber++;
        // the result has always to start with a new row, since the
        // client already writes the header without row separator
        result.append(Configuration.QUERY_RESULT_ROW_SEPARATOR_CHAR);
        String delim = "";
        for (long var : resultVariables) {
          long varResult = mapping.getValue(var, resultVariables);
          if (varResult == -1) {
            throw new RuntimeException("The mapping " + mapping.toString(resultVariables)
                    + " does not contain a mapping for variable " + var + ".");
          }
          Node resultNode = dictionary.decode(varResult);
          if (resultNode == null) {
            throw new RuntimeException("The value " + varResult + " of variable " + var
                    + " could not be found in the dictionary.");
          }
          if (resultNode.isURI()
                  && resultNode.getURI().startsWith(Configuration.BLANK_NODE_URI_PREFIX)) {
            // this is a replacement of a blank node
            resultNode = NodeFactory.createBlankNode(
                    resultNode.getURI().substring(Configuration.BLANK_NODE_URI_PREFIX.length()));
          }
          String resultResourceString = DeSerializer.serializeNode(resultNode);
          result.append(delim).append(resultResourceString);
          delim = Configuration.QUERY_RESULT_COLUMN_SEPARATOR_CHAR;
        }
        if (length > 0) {
          length--;
        }
      } else if (length == 0) {
        break;
      }
    }
    if (result.length() > 0) {
      if (measurementCollector != null) {
        measurementCollector.measureValue(
                MeasurementType.QUERY_COORDINATOR_SEND_QUERY_RESULTS_TO_CLIENT,
                System.currentTimeMillis(), Integer.toString(getQueryId()),
                Long.toString(firstSentResultMappingNumber),
                Long.toString(lastSentResultMappingNumber));
      }
      clientConnections.send(clientId, MessageUtils.createStringMessage(MessageType.QUERY_RESULT,
              result.toString(), logger));
      lastContactWithClient = System.currentTimeMillis();
    } else {
      sendKeepAliveMessageToClient();
    }
    if (length == 0) {
      tidyUp();
      closeInternal();
    }
  }

  private void sendKeepAliveMessageToClient() {
    if ((System.currentTimeMillis()
            - lastContactWithClient) >= Configuration.CLIENT_KEEP_ALIVE_INTERVAL) {
      sendMessageToClient(new byte[] { MessageType.MASTER_WORK_IN_PROGRESS.getValue() });
    }
  }

  private void sendMessageToClient(byte[] message) {
    clientConnections.send(clientId, message);
    lastContactWithClient = System.currentTimeMillis();
  }

  @Override
  protected void executeFinalStep() {
  }

  @Override
  protected void tidyUp() {
    queryExecutionTime = System.currentTimeMillis() - queryExecutionTime;
    sendMessageToClient(new byte[] { MessageType.CLIENT_COMMAND_SUCCEEDED.getValue() });
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.QUERY_COORDINATOR_END,
              System.currentTimeMillis(), Integer.toString(getQueryId()));
    }
    if (logger != null) {
      logger.fine("Query " + getQueryId() + " is finished. Set up time: " + querySetUpTime
              + "ms Execution time: " + queryExecutionTime + "ms");
    }
  }

  @Override
  protected boolean isFinishedLocally() {
    return (numberOfMissingFinishNotificationsFromSlaves == 0) && isInputQueueEmpty(0);
  }

  @Override
  public void close() {
    if (!isInFinalState()) {
      sendMessageToClient(MessageUtils.createStringMessage(MessageType.CLIENT_COMMAND_FAILED,
              "The query has been closed before it was finished.", logger));
      if (logger != null) {
        logger.finer("Query " + getQueryId() + " has been closed before it was finished.");
      }
    }
    closeInternal();
  }

  private void closeInternal() {
    if (!isInFinalState()) {
      messageSender.sendQueryAbortion(getQueryId());
    }
    super.close();
  }

}
