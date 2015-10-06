package de.uni_koblenz.west.cidre.master.client_manager;

import java.io.Closeable;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.messages.MessageUtils;

public class ClientMessageProcessor implements Closeable {

	private final Logger logger;

	private final ClientConnectionManager clientConnections;

	public ClientMessageProcessor(Configuration conf,
			ClientConnectionManager clientConnections, Logger logger) {
		this.logger = logger;
		this.clientConnections = clientConnections;
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
				int newClientID = clientConnections.createConnection(address);
				clientConnections.send(newClientID,
						new byte[] { MessageType.CLIENT_CONNECTION_CONFIRMATION
								.getValue() });
				break;
			case CLIENT_IS_ALIVE:
				if (logger != null) {
					logger.finest("received keep alive from client "
							+ clientConnections.getSenderIdOfLastMessage());
				}
				break;
			case CLIENT_CLOSES_CONNECTION:
				int clientID = clientConnections.getSenderIdOfLastMessage();
				if (logger != null) {
					logger.finer(
							"client " + clientID + " has closed connection");
				}
				clientConnections.closeConnection(clientID);
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
