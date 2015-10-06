package de.uni_koblenz.west.cidre.master.client_manager;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.messages.MessageUtils;

public class ClientMessageProcessor implements Closeable {

	private final Logger logger;

	private final ClientConnectionManager clientConnections;

	private final Map<String, Integer> clientAddress2Id;

	public ClientMessageProcessor(Configuration conf,
			ClientConnectionManager clientConnections, Logger logger) {
		this.logger = logger;
		this.clientConnections = clientConnections;
		clientAddress2Id = new HashMap<>();
	}

	public boolean processMessage() {
		byte[] message = clientConnections.receive();
		if (message != null) {
			switch (MessageType.valueOf(message[0])) {
			case CLIENT_CONNECTION_CREATION:
				String address = MessageUtils.extreactMessageString(message,
						logger);
				if (logger != null) {
					logger.finer("client " + address
							+ " tries to establish a connection");
				}
				int clientID = clientConnections.createConnection(address);
				clientAddress2Id.put(address, clientID);
				clientConnections.send(clientID,
						new byte[] { MessageType.CLIENT_CONNECTION_CONFIRMATION
								.getValue() });
				break;
			case CLIENT_IS_ALIVE:
				address = MessageUtils.extreactMessageString(message, logger);
				if (logger != null) {
					logger.finest("received keep alive from client " + address);
				}
				clientConnections.updateTimerFor(clientAddress2Id.get(address));
				break;
			case CLIENT_CLOSES_CONNECTION:
				address = MessageUtils.extreactMessageString(message, logger);
				if (logger != null) {
					logger.finer(
							"client " + address + " has closed connection");
				}
				clientConnections
						.closeConnection(clientAddress2Id.get(address));
				clientAddress2Id.remove(address);
				break;
			default:
			}
		}
		return message != null;
	}

	@Override
	public void close() {
		clientConnections.close();
	}

}
