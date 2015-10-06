package de.uni_koblenz.west.cidre.master.client_manager;

import java.io.Closeable;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;

public class ClientMessageProcessor implements Closeable {

	private final ClientConnectionManager clientConnections;

	public ClientMessageProcessor(Configuration conf,
			ClientConnectionManager clientConnections, Logger logger,
			String[] currentServer) {
		this.clientConnections = clientConnections;
	}

	// TODO keep alive/timeouts

	public boolean processMessage() {
		byte[] message = clientConnections.receive();

		return message != null;
	}

	@Override
	public void close() {
		clientConnections.close();
	}

}
