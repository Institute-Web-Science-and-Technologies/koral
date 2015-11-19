package de.uni_koblenz.west.cidre.common.query.execution;

import java.io.File;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.executor.WorkerTask;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.messages.MessageUtils;
import de.uni_koblenz.west.cidre.common.query.execution_tree.QueryExecutionTreeType;
import de.uni_koblenz.west.cidre.common.utils.NumberConversion;
import de.uni_koblenz.west.cidre.master.client_manager.ClientConnectionManager;
import de.uni_koblenz.west.cidre.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.cidre.master.statisticsDB.GraphStatistics;

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

	private long currentTaskLoad;

	private int numberOfMissingQueryCreatedMessages;

	private long lastContactWithClient;

	public QueryExecutionCoordinator(short computerID, int queryID,
			int numberOfSlaves, int cacheSize, File cacheDir, int clientID,
			ClientConnectionManager clientConnections,
			DictionaryEncoder dictionary, GraphStatistics statistics,
			Logger logger) {
		super(computerID, queryID, (short) 0, Integer.MAX_VALUE, numberOfSlaves,
				cacheSize, cacheDir);
		this.clientConnections = clientConnections;
		clientId = clientID;
		this.dictionary = dictionary;
		this.statistics = statistics;
		addInputQueue();
		numberOfMissingQueryCreatedMessages = numberOfSlaves;
		lastContactWithClient = System.currentTimeMillis();
	}

	public void processQueryRequest(byte[][] arguments) {
		int queryTypeOctal = NumberConversion.bytes2int(arguments[0]);
		for (QueryExecutionTreeType type : QueryExecutionTreeType.values()) {
			if (type.ordinal() == queryTypeOctal) {
				treeType = type;
				break;
			}
		}
		queryString = MessageUtils.convertToString(arguments[1], logger);
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
		return currentTaskLoad;
	}

	@Override
	public WorkerTask getParentTask() {
		return null;
	}

	@Override
	public void enqueueMessage(long sender, byte[] message, int firstIndex) {
		MessageType mType = MessageType.valueOf(message[firstIndex]);
		switch (mType) {
		case QUERY_CREATED:
			numberOfMissingQueryCreatedMessages--;
			if (numberOfMissingQueryCreatedMessages == 0) {
				messageSender.sendQueryStart(getQueryId());
				sendMessageToClient(MessageUtils.createStringMessage(
						MessageType.MASTER_WORK_IN_PROGRESS,
						"Query execution is started.", logger));
			}
			break;
		case QUERY_TASK_FAILED:
			message[0] = MessageType.CLIENT_COMMAND_FAILED.getValue();
			sendMessageToClient(message);
			closeInternal();
			break;
		default:
			super.enqueueMessage(sender, message, firstIndex);
		}
	}

	@Override
	protected void handleMappingReception(long sender, byte[] message,
			int firstIndex) {
		enqueuMessage(0, message, firstIndex);
	}

	@Override
	protected void executePreStartStep() {
		// send QUERY_CREATE
		// TODO Auto-generated method stub

		sendKeepAliveMessageToClient();
	}

	@Override
	protected void executeOperationStep() {
		// TODO before sending to sparql requester replace urn:blankNode: by _:
		// for proper blank node syntax
		// TODO Auto-generated method stub
		sendKeepAliveMessageToClient();
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
		sendMessageToClient(
				new byte[] { MessageType.CLIENT_COMMAND_SUCCEEDED.getValue() });
	}

	@Override
	protected boolean isFinishedInternal() {
		return true;
	}

	@Override
	public void close() {
		if (!hasFinished()) {
			sendMessageToClient(MessageUtils.createStringMessage(
					MessageType.CLIENT_COMMAND_FAILED,
					"The query has been closed befor it was finished.",
					logger));
		}
		closeInternal();
	}

	private void closeInternal() {
		if (!hasFinished() && !isAborted()) {
			messageSender.sendQueryAbortion(getQueryId());
		}
		super.close();
	}

}
