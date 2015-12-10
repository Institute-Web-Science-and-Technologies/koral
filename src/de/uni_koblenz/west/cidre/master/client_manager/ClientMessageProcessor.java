package de.uni_koblenz.west.cidre.master.client_manager;

import java.io.Closeable;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.messages.MessageUtils;
import de.uni_koblenz.west.cidre.common.query.execution.QueryExecutionCoordinator;
import de.uni_koblenz.west.cidre.common.utils.NumberConversion;
import de.uni_koblenz.west.cidre.common.utils.ReusableIDGenerator;
import de.uni_koblenz.west.cidre.master.CidreMaster;
import de.uni_koblenz.west.cidre.master.tasks.GraphLoaderTask;

/**
 * Processes messages received from a client.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class ClientMessageProcessor
		implements Closeable, ClosedConnectionListener {

	private final Logger logger;

	private final ClientConnectionManager clientConnections;

	private final CidreMaster master;

	private final File tmpDir;

	private final Map<String, Integer> clientAddress2Id;

	private final Map<String, GraphLoaderTask> clientAddress2GraphLoaderTask;

	private final int numberOfChunks;

	private final ReusableIDGenerator queryIdGenerator;

	private final Map<String, QueryExecutionCoordinator> clientAddress2queryExecutionCoordinator;

	private final int mappingReceiverQueueSize;

	private final int emittedMappingsPerRound;

	private final int numberOfHashBuckets;

	private final int maxInMemoryMappings;

	public ClientMessageProcessor(Configuration conf,
			ClientConnectionManager clientConnections, CidreMaster master,
			Logger logger) {
		this.logger = logger;
		this.clientConnections = clientConnections;
		this.master = master;
		numberOfChunks = conf.getNumberOfSlaves();
		tmpDir = new File(conf.getTmpDir());
		if (!tmpDir.exists() || !tmpDir.isDirectory()) {
			throw new IllegalArgumentException("The temporary directory "
					+ conf.getTmpDir() + " is not a directory.");
		}
		clientAddress2Id = new HashMap<>();
		clientAddress2GraphLoaderTask = new HashMap<>();
		clientAddress2queryExecutionCoordinator = new HashMap<>();
		queryIdGenerator = new ReusableIDGenerator();
		this.clientConnections.registerClosedConnectionListener(this);
		mappingReceiverQueueSize = conf.getReceiverQueueSize();
		emittedMappingsPerRound = conf.getMaxEmittedMappingsPerRound();
		numberOfHashBuckets = conf.getNumberOfHashBuckets();
		maxInMemoryMappings = conf.getMaxInMemoryMappingsDuringJoin();
	}

	/**
	 * @param graphHasBeenLoaded
	 * @return <code>true</code>, iff a message was received
	 */
	public boolean processMessage(boolean graphHasBeenLoaded) {
		byte[] message = clientConnections.receive(false);
		if (message != null && message.length > 0) {
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
				case FILE_CHUNK_RESPONSE:
					processFileChunk(message);
					break;
				case CLIENT_COMMAND_ABORTED:
					processAbortCommand(message);
					break;
				case CLIENT_CLOSES_CONNECTION:
					processCloseConnection(message);
					break;
				default:
					if (logger != null) {
						logger.finest(
								"ignoring message with unsupported message type: "
										+ messageType);
					}
					return true;
				}
			} catch (IllegalArgumentException e) {
				if (logger != null) {
					logger.finest("ignoring message with unknown message type: "
							+ message[0]);
					logger.throwing(e.getStackTrace()[0].getClassName(),
							e.getStackTrace()[0].getMethodName(), e);
				}
				return true;
			}
		}
		return message != null;
	}

	private void processCreateConnection(byte[] message) {
		String address = MessageUtils.extractMessageString(message, logger);
		if (logger != null) {
			logger.finer(
					"client " + address + " tries to establish a connection");
		}
		int clientID = clientConnections.createConnection(address);
		clientAddress2Id.put(address, clientID);
		clientConnections.send(clientID, new byte[] {
				MessageType.CLIENT_CONNECTION_CONFIRMATION.getValue() });
	}

	private void processKeepAlive(byte[] message) {
		String address;
		address = MessageUtils.extractMessageString(message, logger);
		Integer cID = clientAddress2Id.get(address);
		if (cID != null) {
			clientConnections.updateTimerFor(cID.intValue());
		} else if (logger != null) {
			logger.finest("ignoring keep alive from client " + address
					+ ". Connection already closed.");
		}
	}

	private void processCommand(byte[] message, boolean graphHasBeenLoaded) {
		byte[] buffer = clientConnections.receive(true);
		if (buffer == null) {
			if (logger != null) {
				logger.finest(
						"Client has sent a command request but missed to send his id.");
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
				logger.finest("Client " + address + " has sent the command "
						+ command
						+ " but did not specify the number of arguments");
			}
			return;
		}
		byte numberOfArguments = buffer[0];

		byte[][] arguments = new byte[numberOfArguments][];
		for (int i = 0; i < numberOfArguments; i++) {
			buffer = clientConnections.receive(true);
			if (buffer == null) {
				if (logger != null) {
					logger.finest("Client " + address + " has sent the command "
							+ command + " that requires " + numberOfArguments
							+ " arguments. But it has received only " + i
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
				logger.finest("The connection to client " + address
						+ " has already been closed.");
			}
			return;
		}

		try {
			switch (command) {
			case "load":
				if (graphHasBeenLoaded) {
					String errorMessage = "Loading of graph rejected: CIDRE is currently loading a graph or it has already loaded a graph.";
					if (logger != null) {
						logger.finer(errorMessage);
					}
					clientConnections.send(clientID,
							MessageUtils.createStringMessage(
									MessageType.CLIENT_COMMAND_FAILED,
									errorMessage, logger));
					break;
				}
				GraphLoaderTask loaderTask = new GraphLoaderTask(
						clientID.intValue(), clientConnections,
						master.getDictionary(), master.getStatistics(), tmpDir,
						master, logger);
				clientAddress2GraphLoaderTask.put(address, loaderTask);
				loaderTask.loadGraph(arguments, numberOfChunks,
						master.getFileSenderConnection());
				break;
			case "query":
				if (!graphHasBeenLoaded) {
					String errorMessage = "There is no graph loaded that could be queried.";
					if (logger != null) {
						logger.finer(errorMessage);
					}
					clientConnections.send(clientID,
							MessageUtils.createStringMessage(
									MessageType.CLIENT_COMMAND_FAILED,
									errorMessage, logger));
					break;
				}
				QueryExecutionCoordinator coordinator = new QueryExecutionCoordinator(
						master.getComputerId(), queryIdGenerator.getNextId(),
						master.getNumberOfSlaves(), mappingReceiverQueueSize,
						tmpDir, clientID.intValue(), clientConnections,
						master.getDictionary(), master.getStatistics(),
						emittedMappingsPerRound, numberOfHashBuckets,
						maxInMemoryMappings, logger);
				coordinator.processQueryRequest(arguments);
				clientAddress2queryExecutionCoordinator.put(address,
						coordinator);
				master.executeTask(coordinator);
				break;
			case "drop":
				processDropTables(clientID);
				break;
			default:
				String errorMessage = "unknown command: " + command + " with "
						+ numberOfArguments + " arguments.";
				if (logger != null) {
					logger.finer(errorMessage);
				}
				clientConnections.send(clientID,
						MessageUtils.createStringMessage(
								MessageType.CLIENT_COMMAND_FAILED, errorMessage,
								logger));
			}
		} catch (RuntimeException e) {
			if (logger != null) {
				logger.finer("error during execution of " + command + " with "
						+ numberOfArguments + " arguments.");
				logger.throwing(e.getStackTrace()[0].getClassName(),
						e.getStackTrace()[0].getMethodName(), e);
			}
			clientConnections.send(clientID, MessageUtils.createStringMessage(
					MessageType.CLIENT_COMMAND_FAILED,
					"error during execution of " + command + " with "
							+ numberOfArguments + " arguments:\n"
							+ e.getClass().getName() + ": " + e.getMessage(),
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
		master.clear();
		clientConnections.send(clientID,
				MessageUtils.createStringMessage(
						MessageType.CLIENT_COMMAND_SUCCEEDED,
						"Database is dropped, successfully.", logger));
		if (logger != null) {
			logger.finer("Database is dropped.");
		}
	}

	private void processFileChunk(byte[] message) {
		byte[] buffer = clientConnections.receive(true);
		if (buffer == null) {
			if (logger != null) {
				logger.finest(
						"Client has sent a command request but missed to send his id.");
			}
			return;
		}
		String address = MessageUtils.convertToString(buffer, logger);

		buffer = clientConnections.receive(true);
		if (buffer == null || buffer.length != 4) {
			if (logger != null) {
				logger.finest("Client " + address
						+ " has not sent the id of the file this chunk belongs to.");
			}
			return;
		}
		int fileID = NumberConversion.bytes2int(buffer);

		buffer = clientConnections.receive(true);
		if (buffer == null) {
			if (logger != null) {
				logger.finest("Client " + address
						+ " has not sent the id of the chunk.");
			}
			return;
		}
		long chunkID = NumberConversion.bytes2long(buffer);

		buffer = clientConnections.receive(true);
		if (buffer == null || buffer.length != 8) {
			if (logger != null) {
				logger.finest("Client " + address
						+ " has not sent the total number of chunks.");
			}
			return;
		}
		long totalNumberOfChunks = NumberConversion.bytes2long(buffer);

		buffer = clientConnections.receive(true);
		if (buffer == null) {
			if (logger != null) {
				logger.finest("Client " + address
						+ " has not sent the content of chunk " + chunkID
						+ " of file" + fileID + ".");
			}
			return;
		}

		if (address.trim().isEmpty()) {
			if (logger != null) {
				logger.finest("Client has not sent his address.");
			}
			return;
		}
		Integer clientID = clientAddress2Id.get(address);
		if (clientID == null) {
			if (logger != null) {
				logger.finest("The connection to client " + address
						+ " has already been closed.");
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
		task.receiveFileChunk(fileID, chunkID, totalNumberOfChunks, buffer);
	}

	private void processAbortCommand(byte[] message) {
		String abortionContext = MessageUtils.extractMessageString(message,
				logger);
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
		QueryExecutionCoordinator query = clientAddress2queryExecutionCoordinator
				.get(address);
		if (query != null) {
			query.close();
			queryIdGenerator.release(query.getQueryId());
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
		for (QueryExecutionCoordinator task : clientAddress2queryExecutionCoordinator
				.values()) {
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
			if (entry.getValue() != null
					&& entry.getValue().intValue() == clientID) {
				address = entry.getKey();
			}
		}
		if (address != null) {
			terminateTask(address);
			clientAddress2Id.remove(address);
		}
	}

	public boolean isGraphLoaded(boolean graphHasBeenLoaded) {
		Entry<String, GraphLoaderTask> task = null;
		for (Entry<String, GraphLoaderTask> entry : clientAddress2GraphLoaderTask
				.entrySet()) {
			if (entry.getValue() != null
					&& entry.getValue() instanceof GraphLoaderTask) {
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
