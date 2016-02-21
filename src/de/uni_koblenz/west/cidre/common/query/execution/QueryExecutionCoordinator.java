package de.uni_koblenz.west.cidre.common.query.execution;

import java.io.File;
import java.util.logging.Logger;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.executor.WorkerTask;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.messages.MessageUtils;
import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.parser.QueryExecutionTreeType;
import de.uni_koblenz.west.cidre.common.query.parser.SparqlParser;
import de.uni_koblenz.west.cidre.common.query.parser.VariableDictionary;
import de.uni_koblenz.west.cidre.common.utils.NumberConversion;
import de.uni_koblenz.west.cidre.master.client_manager.ClientConnectionManager;
import de.uni_koblenz.west.cidre.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.cidre.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.cidre.master.utils.DeSerializer;

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

	public QueryExecutionCoordinator(short computerID, int queryID,
			int numberOfSlaves, int cacheSize, File cacheDir, int clientID,
			ClientConnectionManager clientConnections,
			DictionaryEncoder dictionary, GraphStatistics statistics,
			int emittedMappingsPerRound, int numberOfHashBuckets,
			int maxInMemoryMappings, Logger logger) {
		super(computerID, queryID, (short) 0, numberOfSlaves, cacheSize,
				cacheDir);
		this.logger = logger;
		setEstimatedWorkLoad(Integer.MAX_VALUE);
		this.clientConnections = clientConnections;
		clientId = clientID;
		this.dictionary = dictionary;
		this.statistics = statistics;
		addInputQueue();
		numberOfMissingQueryCreatedMessages = numberOfSlaves;
		numberOfMissingFinishNotificationsFromSlaves = numberOfSlaves;
		lastContactWithClient = System.currentTimeMillis();
		varDictionary = new VariableDictionary();
		this.emittedMappingsPerRound = emittedMappingsPerRound;
		parser = new SparqlParser(dictionary, null, computerID, getQueryId(),
				getID(), numberOfSlaves, cacheSize, cacheDir,
				emittedMappingsPerRound, numberOfHashBuckets,
				maxInMemoryMappings, false);
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
			logger.fine("Started query coordinator for query "
					+ queryString.replace('\n', ' '));
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
	public void enqueueMessage(long sender, byte[] message, int firstIndex,
			int messageLength) {
		MessageType mType = MessageType.valueOf(message[firstIndex]);
		switch (mType) {
		case QUERY_CREATED:
			numberOfMissingQueryCreatedMessages--;
			if (numberOfMissingQueryCreatedMessages == 0) {
				start();
				messageSender.sendQueryStart(getQueryId());
				sendMessageToClient(MessageUtils.createStringMessage(
						MessageType.MASTER_WORK_IN_PROGRESS,
						"Query execution is started.", logger));
				if (logger != null) {
					logger.finer("Query " + getQueryId()
							+ " has been created on all slaves. Start of execution.");
				}
				sendMessageToClient(MessageUtils.createStringMessage(
						MessageType.MASTER_WORK_IN_PROGRESS,
						"Query execution tree has been created on all slaves. Start of execution.",
						logger));
			}
			break;
		case QUERY_TASK_FAILED:
			if (logger != null) {
				logger.finer("Query " + getQueryId() + " failed.");
			}
			byte[] result = new byte[messageLength - Long.BYTES - Short.BYTES];
			result[0] = MessageType.CLIENT_COMMAND_FAILED.getValue();
			System.arraycopy(message,
					firstIndex + Byte.BYTES + Long.BYTES + Short.BYTES, result,
					Byte.BYTES, result.length - Byte.BYTES);
			sendMessageToClient(result);
			closeInternal();
			break;
		default:
			super.enqueueMessage(sender, message, firstIndex, messageLength);
		}
	}

	@Override
	protected void handleFinishNotification(long sender, Object object,
			int firstIndex, int messageLength) {
		numberOfMissingFinishNotificationsFromSlaves--;
		if (logger != null) {
			// TODO remove
			logger.info(NumberConversion.id2description(getID())
					+ " receives finish notification. Missing notifications = "
					+ numberOfMissingFinishNotificationsFromSlaves);
		}
	}

	@Override
	protected void handleMappingReception(long sender, byte[] message,
			int firstIndex, int length) {
		enqueuMessageInternal(0, message, firstIndex, length);
	}

	@Override
	protected void executePreStartStep() {
		if (parser != null) {
			QueryOperatorBase queryExecutionTree = (QueryOperatorBase) parser
					.parse(queryString, treeType, varDictionary);
			resultVariables = queryExecutionTree.getResultVariables();
			messageSender.sendQueryCreate(statistics, getQueryId(),
					queryExecutionTree, parser.isBaseImplementationUsed());
			parser = null;
		}
		sendKeepAliveMessageToClient();
	}

	@Override
	protected void executeOperationStep() {
		StringBuilder result = new StringBuilder();
		for (int numberOfAlreadyEmittedMessages = 0; numberOfAlreadyEmittedMessages < emittedMappingsPerRound; numberOfAlreadyEmittedMessages++) {
			Mapping mapping = consumeMapping(0);
			if (mapping == null) {
				break;
			}
			// the result has always to start with a new row, since the client
			// already writes the header without row separator
			result.append(Configuration.QUERY_RESULT_ROW_SEPARATOR_CHAR);
			String delim = "";
			for (long var : resultVariables) {
				long varResult = mapping.getValue(var, resultVariables);
				if (varResult == -1) {
					throw new RuntimeException(
							"The mapping " + mapping.toString(resultVariables)
									+ " does not contain a mapping for variable "
									+ var + ".");
				}
				Node resultNode = dictionary.decode(varResult);
				if (resultNode == null) {
					throw new RuntimeException(
							"The value " + varResult + " of variable " + var
									+ " could not be found in the dictionary.");
				}
				if (resultNode.isURI() && resultNode.getURI()
						.startsWith(Configuration.BLANK_NODE_URI_PREFIX)) {
					// this is a replacement of a blank node
					resultNode = NodeFactory.createBlankNode(resultNode.getURI()
							.substring(Configuration.BLANK_NODE_URI_PREFIX
									.length()));
				}
				String resultResourceString = DeSerializer
						.serializeNode(resultNode);
				result.append(delim).append(resultResourceString);
				delim = Configuration.QUERY_RESULT_COLUMN_SEPARATOR_CHAR;
			}
		}
		if (result.length() > 0) {
			clientConnections.send(clientId, MessageUtils.createStringMessage(
					MessageType.QUERY_RESULT, result.toString(), logger));
			lastContactWithClient = System.currentTimeMillis();
		} else {
			sendKeepAliveMessageToClient();
		}
	}

	private void sendKeepAliveMessageToClient() {
		if (System.currentTimeMillis()
				- lastContactWithClient >= Configuration.CLIENT_KEEP_ALIVE_INTERVAL) {
			sendMessageToClient(new byte[] {
					MessageType.MASTER_WORK_IN_PROGRESS.getValue() });
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
		if (logger != null) {
			// TODO remove
			logger.info(NumberConversion.id2description(getID())
					+ " sends finish notification to client");
		}
		sendMessageToClient(
				new byte[] { MessageType.CLIENT_COMMAND_SUCCEEDED.getValue() });
	}

	@Override
	protected boolean isFinishedInternal() {
		return numberOfMissingFinishNotificationsFromSlaves == 0;
	}

	@Override
	public void close() {
		if (!hasFinished()) {
			sendMessageToClient(MessageUtils.createStringMessage(
					MessageType.CLIENT_COMMAND_FAILED,
					"The query has been closed before it was finished.",
					logger));
			if (logger != null) {
				logger.finer("Query " + getQueryId()
						+ " has been closed before it was finished.");
			}
		}
		closeInternal();
	}

	private void closeInternal() {
		if (!hasFinished()) {
			messageSender.sendQueryAbortion(getQueryId());
		}
		super.close();
	}

}
