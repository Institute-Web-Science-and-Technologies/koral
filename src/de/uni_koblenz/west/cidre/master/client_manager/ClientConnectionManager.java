package de.uni_koblenz.west.cidre.master.client_manager;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.networManager.NetworkContextFactory;

public class ClientConnectionManager implements Closeable {

	private final Logger logger;

	private final ZContext context;

	private Socket receiver;

	private final List<Socket> clientConnections;

	private int nextReceiver;

	public ClientConnectionManager(Configuration conf, Logger logger,
			String[] currentServer) {
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
		nextReceiver = -1;
	}

	public byte[] receive() {
		byte[] message = null;
		if (nextReceiver == -1) {
			message = receiver.recv(ZMQ.DONTWAIT);
			nextReceiver++;
		}
		for (; message == null
				&& nextReceiver < clientConnections.size(); nextReceiver++) {
			message = clientConnections.get(nextReceiver).recv(ZMQ.DONTWAIT);
		}

		if (nextReceiver > clientConnections.size() - 1) {
			nextReceiver = -1;
		}
		return message;
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
				return i;
			}
		}
		clientConnections.add(socket);

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
		Socket socket = clientConnections.get(clientID);
		socket.close();

		if (logger != null) {
			logger.finer("connection to client " + clientID + " closed.");
		}

		clientConnections.set(clientID, null);
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
