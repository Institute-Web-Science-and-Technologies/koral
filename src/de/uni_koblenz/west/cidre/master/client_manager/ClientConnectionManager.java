package de.uni_koblenz.west.cidre.master.client_manager;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.networManager.NetworkContextFactory;

public class ClientConnectionManager implements Closeable {

	private final Logger logger;

	private final ZContext context;

	private Socket receiver;

	private final List<Socket> clientConnections;

	private final List<Long> latestLifeSignalTimeFromClient;

	private int nextReceiver;

	private final long connectionTimeout;

	public ClientConnectionManager(Configuration conf, Logger logger) {
		this.logger = logger;
		String[] client = conf.getClient();
		context = NetworkContextFactory.getNetworkContext();

		receiver = context.createSocket(ZMQ.PULL);
		receiver.bind("tcp://" + client[0] + ":" + client[1]);

		if (logger != null) {
			logger.info("client manager listening on tcp://" + client[0] + ":"
					+ client[1]);
		}

		clientConnections = new ArrayList<>();
		latestLifeSignalTimeFromClient = new ArrayList<>();
		nextReceiver = -1;

		connectionTimeout = conf.getClientConnectionTimeout();
	}

	public byte[] receive() {
		byte[] message = null;
		int startPoint = nextReceiver;
		while (message == null) {
			if (nextReceiver == -1) {
				message = receiver.recv(ZMQ.DONTWAIT);
			} else if (nextReceiver < clientConnections.size()) {
				message = clientConnections.get(nextReceiver)
						.recv(ZMQ.DONTWAIT);
				if (message != null) {
					// reset timer for connection
					latestLifeSignalTimeFromClient.set(nextReceiver,
							new Long(System.currentTimeMillis()));
				} else if (System.currentTimeMillis()
						- latestLifeSignalTimeFromClient
								.get(nextReceiver) >= connectionTimeout) {
					// The connection has to be closed due to a timeout
					if (logger != null) {
						logger.finer("Timeout for client connection "
								+ nextReceiver);
					}
					closeConnection(nextReceiver);
				}
			}
			nextReceiver++;
			if (nextReceiver > clientConnections.size() - 1) {
				nextReceiver = -1;
			}

			if (nextReceiver == startPoint) {
				// one round trip has been finished
				break;
			}
		}
		return message;
	}

	public int getSenderIdOfLastMessage() {
		return nextReceiver == -1 ? clientConnections.size() - 1
				: nextReceiver - 1;
	}

	public int createConnection(String clientIPAndPort) {
		Socket socket = context.createSocket(ZMQ.PUSH);
		socket.connect(clientIPAndPort);
		for (int i = 0; i < clientConnections.size(); i++) {
			if (clientConnections.get(i) == null) {
				if (logger != null) {
					logger.finer("connected to client " + i + ": "
							+ clientIPAndPort);
				}
				clientConnections.set(i, socket);
				latestLifeSignalTimeFromClient.set(i,
						new Long(System.currentTimeMillis()));
				return i;
			}
		}
		clientConnections.add(socket);
		latestLifeSignalTimeFromClient
				.add(new Long(System.currentTimeMillis()));

		if (logger != null) {
			logger.finer("connected to client " + (clientConnections.size() - 1)
					+ ": " + clientIPAndPort);
		}

		return clientConnections.size() - 1;
	}

	public void send(int receivingClient, byte[] message) {
		Socket out = clientConnections.get(receivingClient);
		if (out != null) {
			synchronized (out) {
				out.send(message);
			}
		}
	}

	public void sendToAll(byte[] message) {
		for (Socket socket : clientConnections) {
			if (socket != null) {
				synchronized (socket) {
					socket.send(message);
				}
			}
		}
	}

	public void closeConnection(int clientID) {
		send(clientID, new byte[] { MessageType.CONNECTION_CLOSED.getValue() });
		Socket socket = clientConnections.get(clientID);
		socket.close();

		if (logger != null) {
			logger.finer("connection to client " + clientID + " closed.");
		}

		clientConnections.set(clientID, null);
		latestLifeSignalTimeFromClient.set(clientID, null);
	}

	@Override
	public void close() {
		for (int i = 0; i < clientConnections.size(); i++) {
			closeConnection(i);
		}
		receiver.close();
		receiver = null;
		NetworkContextFactory.destroyNetworkContext(context);
	}

}
