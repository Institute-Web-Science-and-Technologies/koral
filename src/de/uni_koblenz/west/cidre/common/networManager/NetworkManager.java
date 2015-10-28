package de.uni_koblenz.west.cidre.common.networManager;

import java.io.Closeable;
import java.util.Arrays;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;

/**
 * First slave has id 1!!
 */
public class NetworkManager implements Closeable {

	private final ZContext context;

	private Socket receiver;

	private final Socket[] senders;

	private int currentID;

	public NetworkManager(Configuration conf, String[] currentServer) {
		context = NetworkContextFactory.getNetworkContext();

		receiver = context.createSocket(ZMQ.PULL);
		receiver.bind("tcp://" + currentServer[0] + ":" + currentServer[1]);

		senders = new Socket[conf.getNumberOfSlaves() + 1];
		String[] master = conf.getMaster();
		senders[0] = context.createSocket(ZMQ.PUSH);

		senders[0].connect("tcp://" + master[0] + ":" + master[1]);
		if (Arrays.equals(currentServer, master)) {
			currentID = 0;
		}
		for (int i = 1; i < senders.length; i++) {
			String[] slave = conf.getSlave(i - 1);
			senders[i] = context.createSocket(ZMQ.PUSH);
			senders[i].connect("tcp://" + slave[0] + ":" + slave[1]);
			if (Arrays.equals(currentServer, slave)) {
				currentID = i;
			}
		}
	}

	public int getCurrentID() {
		return currentID;
	}

	public void sendMore(int receiver, byte[] message) {
		Socket out = senders[receiver];
		if (out != null) {
			out.sendMore(message);
		}
	}

	public void send(int receiver, byte[] message) {
		Socket out = senders[receiver];
		if (out != null) {
			out.send(message);
		}
	}

	public void broadcastToAllOtherSlaves(byte[] message) {
		for (int i = 1; i < senders.length; i++) {
			if (i == currentID) {
				// do not broadcast a message to oneself
				continue;
			}
			Socket out = senders[i];
			if (out != null) {
				out.send(message);
			}
		}
	}

	public byte[] receive() {
		return receive(false);
	}

	public byte[] receive(boolean waitForResponse) {
		if (receiver != null) {
			if (waitForResponse) {
				return receiver.recv();
			} else {
				return receiver.recv(ZMQ.DONTWAIT);
			}
		} else {
			return null;
		}
	}

	public int getNumberOfSlaves() {
		return senders.length - 1;
	}

	@Override
	public void close() {
		for (int i = 0; i < senders.length; i++) {
			context.destroySocket(senders[i]);
			senders[i] = null;
		}
		context.destroySocket(receiver);
		receiver = null;
		NetworkContextFactory.destroyNetworkContext(context);
	}

}
