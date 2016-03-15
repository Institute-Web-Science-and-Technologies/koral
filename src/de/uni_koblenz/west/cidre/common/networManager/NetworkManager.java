package de.uni_koblenz.west.cidre.common.networManager;

import java.io.Closeable;
import java.util.Arrays;
import java.util.logging.Logger;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.executor.messagePassing.MessageSender;

/**
 * Creates connections between the CIDRE components, i.e, master and slaves.
 * Furthermore, it allows sending messages to specific component. Therefore, the
 * master has the id 0 and the slaves ids &gt;=1. Additionally it provides
 * methods for receiving messages.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class NetworkManager implements Closeable, MessageSender {

	// TODO remove
	public Logger logger;

	private final static int SEND_TIMEOUT = 100;

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
		senders[0].setSendTimeOut(SEND_TIMEOUT);
		senders[0].connect("tcp://" + master[0] + ":" + master[1]);
		if (Arrays.equals(currentServer, master)) {
			currentID = 0;
		}
		for (int i = 1; i < senders.length; i++) {
			String[] slave = conf.getSlave(i - 1);
			senders[i] = context.createSocket(ZMQ.PUSH);
			senders[i].setSendTimeOut(SEND_TIMEOUT);
			senders[i].connect("tcp://" + slave[0] + ":" + slave[1]);
			if (Arrays.equals(currentServer, slave)) {
				currentID = i;
			}
		}
	}

	@Override
	public int getCurrentID() {
		return currentID;
	}

	public void sendMore(int receiver, byte[] message) {
		if (logger != null) {
			// TODO remove
			logger.info("sendMore() started "
					+ Arrays.toString(Thread.currentThread().getStackTrace()));
		}
		Socket out = senders[receiver];
		if (out != null) {
			synchronized (out) {
				boolean wasSent = false;
				while (!wasSent) {
					wasSent = out.sendMore(message);
				}
			}
		}
		if (logger != null) {
			// TODO remove
			logger.info("sendMore() finished");
		}
	}

	@Override
	public void send(int receiver, byte[] message) {
		if (logger != null) {
			// TODO remove
			logger.info("send() started "
					+ Arrays.toString(Thread.currentThread().getStackTrace()));
		}
		Socket out = senders[receiver];
		if (out != null) {
			synchronized (out) {
				boolean wasSent = false;
				while (!wasSent) {
					wasSent = out.send(message);
				}
			}
		}
		if (logger != null) {
			// TODO remove
			logger.info("send() finished");
		}
	}

	public void sendToAll(byte[] message) {
		sendToAll(message, Integer.MIN_VALUE, false);
	}

	@Override
	public void sendToAllSlaves(byte[] message) {
		sendToAll(message, Integer.MIN_VALUE, true);
	}

	@Override
	public void sendToAllOtherSlaves(byte[] message) {
		sendToAll(message, currentID, true);
	}

	private void sendToAll(byte[] message, int excludedSlave,
			boolean excludeMaster) {
		for (int i = excludeMaster ? 1 : 0; i < senders.length; i++) {
			if (i == excludedSlave) {
				// do not broadcast a message to excluded slave
				continue;
			}
			if (logger != null) {
				// TODO remove
				logger.info("sendToAll() started " + Arrays
						.toString(Thread.currentThread().getStackTrace()));
			}
			Socket out = senders[i];
			if (out != null) {
				synchronized (out) {
					boolean wasSent = false;
					while (!wasSent) {
						wasSent = out.send(message);
					}
				}
			}
			if (logger != null) {
				// TODO remove
				logger.info("sendToAll() finished");
			}
		}
	}

	public byte[] receive() {
		return receive(false);
	}

	public byte[] receive(boolean waitForResponse) {
		if (receiver != null) {
			if (waitForResponse) {
				if (logger != null) {
					// TODO remove
					logger.info("receive(" + waitForResponse + ") started "
							+ Arrays.toString(
									Thread.currentThread().getStackTrace()));
				}
				synchronized (receiver) {
					byte[] message = receiver.recv();
					if (logger != null) {
						// TODO remove
						logger.info(
								"receive(" + waitForResponse + ") finished");
					}
					return message;
				}
			} else {
				synchronized (receiver) {
					byte[] message = receiver.recv(ZMQ.DONTWAIT);
					return message;
				}
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
			if (senders[i] != null) {
				synchronized (senders[i]) {
					context.destroySocket(senders[i]);
					senders[i] = null;
				}
			}
		}
		synchronized (receiver) {
			context.destroySocket(receiver);
		}
		receiver = null;
		NetworkContextFactory.destroyNetworkContext(context);
	}

}
