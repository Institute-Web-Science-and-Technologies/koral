package de.uni_koblenz.west.cidre.master.client_manager;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.messages.MessageUtils;
import de.uni_koblenz.west.cidre.master.tasks.GraphLoaderTask;

public class ClientMessageProcessor
		implements Closeable, ClosedConnectionListener {

	private final Logger logger;

	private final ClientConnectionManager clientConnections;

	private final File tmpDir;

	private final Map<String, Integer> clientAddress2Id;

	private final Map<String, GraphLoaderTask> clientAddress2GraphLoaderTask;

	public ClientMessageProcessor(Configuration conf,
			ClientConnectionManager clientConnections, Logger logger) {
		this.logger = logger;
		this.clientConnections = clientConnections;
		tmpDir = new File(conf.getTmpDir());
		if (!tmpDir.exists() || !tmpDir.isDirectory()) {
			throw new IllegalArgumentException("The temporary directory "
					+ conf.getTmpDir() + " is not a directory.");
		}
		clientAddress2Id = new HashMap<>();
		clientAddress2GraphLoaderTask = new HashMap<>();
		this.clientConnections.addClosedConnectionListener(this);
	}

	/**
	 * @return <code>true</code>, iff a message was received
	 */
	public boolean processMessage() {
		byte[] message = clientConnections.receive(false);
		if (message != null) {
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
					processCommand(message);
					break;
				case FILE_CHUNK:
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
		String address = MessageUtils.extreactMessageString(message, logger);
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
		address = MessageUtils.extreactMessageString(message, logger);
		if (logger != null) {
			logger.finest("received keep alive from client " + address);
		}
		Integer cID = clientAddress2Id.get(address);
		if (cID != null) {
			clientConnections.updateTimerFor(cID.intValue());
		} else if (logger != null) {
			logger.finest("ignoring keep alive from client " + address
					+ ". Connection already closed.");
		}
	}

	private void processCommand(byte[] message) {
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
				GraphLoaderTask loaderTask = new GraphLoaderTask(
						clientID.intValue(), clientConnections, tmpDir, logger);
				clientAddress2GraphLoaderTask.put(address, loaderTask);
				loaderTask.loadGraph(arguments);
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
			if (command.equals("load")) {
				terminateTask(address);
			}
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
		if (buffer == null) {
			if (logger != null) {
				logger.finest("Client " + address
						+ " has not sent the id of the file this chunk belongs to.");
			}
			return;
		}
		int fileID = ByteBuffer.wrap(buffer).getInt();

		buffer = clientConnections.receive(true);
		if (buffer == null) {
			if (logger != null) {
				logger.finest("Client " + address
						+ " has not sent the id of the chunk.");
			}
			return;
		}
		long chunkID = ByteBuffer.wrap(buffer).getLong();

		buffer = clientConnections.receive(true);
		if (buffer == null) {
			if (logger != null) {
				logger.finest("Client " + address
						+ " has not sent the total number of chunks.");
			}
			return;
		}
		long totalNumberOfChunks = ByteBuffer.wrap(buffer).getLong();

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
		if (logger != null) {
			logger.finest("received file chunk from client " + address);
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
		String abortionContext = MessageUtils.extreactMessageString(message,
				logger);
		String[] parts = abortionContext.split(Pattern.quote("|"));
		if (logger != null) {
			logger.finer("client " + parts[0] + " aborts command " + parts[1]);
		}
		switch (parts[1].toLowerCase()) {
		case "load":
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
			if (task.isAlive()) {
				task.interrupt();
			}
			task.close();
			clientAddress2GraphLoaderTask.remove(address);
		}
	}

	private void processCloseConnection(byte[] message) {
		String address;
		Integer cID;
		address = MessageUtils.extreactMessageString(message, logger);
		if (logger != null) {
			logger.finer("client " + address + " has closed connection");
		}
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
		for (GraphLoaderTask task : clientAddress2GraphLoaderTask.values()) {
			if (task != null) {
				if (task.isAlive()) {
					task.interrupt();
				}
				task.close();
			}
		}
		clientConnections.close();
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

}
