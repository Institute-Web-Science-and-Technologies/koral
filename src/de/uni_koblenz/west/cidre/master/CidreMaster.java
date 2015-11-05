package de.uni_koblenz.west.cidre.master;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.fileTransfer.FileSenderConnection;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.query.messagePassing.MessageReceiverListener;
import de.uni_koblenz.west.cidre.common.system.CidreSystem;
import de.uni_koblenz.west.cidre.master.client_manager.ClientConnectionManager;
import de.uni_koblenz.west.cidre.master.client_manager.ClientMessageProcessor;
import de.uni_koblenz.west.cidre.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.cidre.master.networkManager.MasterNetworkManager;
import de.uni_koblenz.west.cidre.master.statisticsDB.GraphStatistics;

public class CidreMaster extends CidreSystem {

	private final ClientMessageProcessor clientMessageProcessor;

	private final DictionaryEncoder dictionary;

	private final GraphStatistics statistics;

	private boolean graphHasBeenLoaded;

	public CidreMaster(Configuration conf) {
		super(conf, conf.getMaster(),
				new MasterNetworkManager(conf, conf.getMaster()));
		try {
			ClientConnectionManager clientConnections = new ClientConnectionManager(
					conf, logger);
			dictionary = new DictionaryEncoder(conf, logger);
			statistics = new GraphStatistics(conf,
					(short) conf.getNumberOfSlaves(), logger);
			clientMessageProcessor = new ClientMessageProcessor(conf,
					clientConnections, this, logger);
			graphHasBeenLoaded = !dictionary.isEmpty();
		} catch (Throwable t) {
			if (logger != null) {
				logger.throwing(t.getStackTrace()[0].getClassName(),
						t.getStackTrace()[0].getMethodName(), t);
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

	public FileSenderConnection getFileSenderConnection() {
		return (MasterNetworkManager) super.getNetworkManager();
	}

	// TODO before sending to sparql reqester replace urn:blankNode: by _: for
	// proper blank node syntax

	@Override
	public void runOneIteration() {
		boolean messageReceived = false;
		// process client message
		messageReceived = clientMessageProcessor
				.processMessage(graphHasBeenLoaded);
		graphHasBeenLoaded = clientMessageProcessor
				.isGraphLoaded(graphHasBeenLoaded);
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
		if (receivedMessage == null || receivedMessage.length == 0) {
			return;
		}
		MessageType messageType = null;
		try {
			messageType = MessageType.valueOf(receivedMessage[0]);
			switch (messageType) {
			case FILE_CHUNK_REQUEST:
				byte[][] message = new byte[4][];
				message[0] = new byte[] { receivedMessage[0] };
				message[1] = new byte[2];
				System.arraycopy(receivedMessage, 1, message[1], 0,
						message[1].length);
				message[2] = new byte[4];
				System.arraycopy(receivedMessage, 3, message[2], 0,
						message[2].length);
				message[3] = new byte[8];
				System.arraycopy(receivedMessage, 7, message[3], 0,
						message[3].length);
				short slaveID = ByteBuffer.wrap(message[1]).getShort();
				notifyMessageListener(messageType.getListenerType(), slaveID,
						message);
				break;
			case GRAPH_LOADING_COMPLETE:
				message = new byte[2][];
				message[0] = new byte[] { receivedMessage[0] };
				message[1] = new byte[2];
				System.arraycopy(receivedMessage, 1, message[1], 0,
						message[1].length);
				slaveID = ByteBuffer.wrap(message[1]).getShort();
				notifyMessageListener(messageType.getListenerType(), slaveID,
						message);
				break;
			case GRAPH_LOADING_FAILED:
				message = new byte[3][];
				message[0] = new byte[] { receivedMessage[0] };
				message[1] = Arrays.copyOfRange(receivedMessage, 1, 3);
				message[2] = Arrays.copyOfRange(receivedMessage, 3,
						receivedMessage.length);
				slaveID = ByteBuffer.wrap(message[1]).getShort();
				notifyMessageListener(messageType.getListenerType(), slaveID,
						message);
				break;
			case QUERY_CREATED:
			case QUERY_MAPPING_BATCH:
			case QUERY_TASK_FINISHED:
			case QUERY_TASK_FAILED:
				short senderID = ByteBuffer.wrap(receivedMessage, 1, 2)
						.getShort();
				notifyMessageListener(MessageReceiverListener.class, senderID,
						receivedMessage);
				break;
			default:
				if (logger != null) {
					logger.finer("Unknown message type received from slave: "
							+ messageType.name());
				}
			}
		} catch (IllegalArgumentException e) {
			if (logger != null) {
				logger.finer("Unknown message type: " + receivedMessage[0]);
				logger.throwing(e.getStackTrace()[0].getClassName(),
						e.getStackTrace()[0].getMethodName(), e);
			}
		} catch (BufferUnderflowException | IndexOutOfBoundsException e) {
			if (logger != null) {
				logger.finer("Message of type " + messageType
						+ " is too short with only " + receivedMessage.length
						+ " received bytes.");
				logger.throwing(e.getStackTrace()[0].getClassName(),
						e.getStackTrace()[0].getMethodName(), e);
			}
		}
	}

	@Override
	public void clearInternal() {
		getNetworkManager()
				.sendToAllSlaves(new byte[] { MessageType.CLEAR.getValue() });
		clientMessageProcessor.clear();
		dictionary.clear();
		statistics.clear();
		graphHasBeenLoaded = false;
		if (logger != null) {
			logger.info("master cleared");
		}
	}

	@Override
	protected void shutDownInternal() {
		clientMessageProcessor.close();
		dictionary.close();
		statistics.close();
	}

	public static void main(String[] args) {
		String className = CidreMaster.class.getName();
		String additionalArgs = "";
		Options options = createCommandLineOptions();
		try {
			CommandLine line = parseCommandLineArgs(options, args);
			Configuration conf = initializeConfiguration(options, line,
					className, additionalArgs);

			CidreMaster master = new CidreMaster(conf);
			master.start();

		} catch (ParseException e) {
			e.printStackTrace();
			printUsage(className, options, additionalArgs);
		}
	}

}
