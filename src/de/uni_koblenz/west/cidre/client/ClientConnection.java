package de.uni_koblenz.west.cidre.client;

import java.io.Closeable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.messages.MessageUtils;
import de.uni_koblenz.west.cidre.common.networManager.NetworkContextFactory;

public class ClientConnection implements Closeable {

	private final ZContext context;

	private Socket outSocket;

	private Socket inSocket;

	private String clientAddress;

	public ClientConnection() {
		context = NetworkContextFactory.getNetworkContext();
	}

	public void connect(String masterAddress) {
		System.out.println("Connecting to master...");
		outSocket = context.createSocket(ZMQ.PUSH);
		outSocket.connect("tcp://" + masterAddress);
		if (inSocket == null) {
			inSocket = context.createSocket(ZMQ.PULL);
			try {
				String hostAddress = getHostAddress();
				int port = inSocket.bindToRandomPort("tcp://" + hostAddress,
						49152, 61000);
				clientAddress = hostAddress + ":" + port;

				// exchange a unique connection with master
				outSocket.send(MessageUtils.createStringMessage(
						MessageType.CLIENT_CONNECTION_CREATION, clientAddress,
						null));
				inSocket.setReceiveTimeOut(
						(int) Configuration.CLIENT_CONNECTION_TIMEOUT);
				byte[] answer = inSocket.recv();
				if (answer == null
						|| (answer.length != 1 && MessageType.valueOf(
								answer[0]) != MessageType.CLIENT_CONNECTION_CONFIRMATION)) {
					System.out.println(
							"Master is not confirming connection attempt.");
					closeConnectionToMaster();
					return;
				}
				new Thread() {
					@Override
					public void run() {
						while (!isInterrupted() && inSocket != null) {
							long startTime = System.currentTimeMillis();
							outSocket.send(MessageUtils.createStringMessage(
									MessageType.CLIENT_IS_ALIVE, clientAddress,
									null));
							long remainingSleepTime = Configuration.CLIENT_KEEP_ALIVE_INTERVAL
									- System.currentTimeMillis() + startTime;
							if (remainingSleepTime > 0) {
								try {
									Thread.sleep(remainingSleepTime);
								} catch (InterruptedException e) {
								}
							}
						}
					}
				}.start();
			} catch (UnknownHostException e) {
				System.out.println(
						"Connection failed because the local IP address could not be identified.");
				throw new RuntimeException(e);
			}
		}
		System.out.println("Connection established.");
	}

	private String getHostAddress() throws UnknownHostException {
		InetAddress localHost = InetAddress.getLocalHost();
		if (localHost instanceof Inet4Address
				&& !localHost.isLoopbackAddress()) {
			return localHost.getHostAddress();
		} else {
			try {
				Enumeration<NetworkInterface> networkInterfaces = NetworkInterface
						.getNetworkInterfaces();
				while (networkInterfaces.hasMoreElements()) {
					NetworkInterface netIface = networkInterfaces.nextElement();
					Enumeration<InetAddress> addresses = netIface
							.getInetAddresses();
					while (addresses.hasMoreElements()) {
						InetAddress addr = addresses.nextElement();
						if (addr instanceof Inet4Address
								&& !addr.isLoopbackAddress()) {
							return addr.getHostAddress();
						}
					}
				}
			} catch (SocketException e1) {
				System.out.println(
						"Connection failed because the local IP address could not be identified.");
				throw new RuntimeException(e1);
			}
			return null;
		}
	}

	private void closeConnectionToMaster() {
		outSocket.send(MessageUtils.createStringMessage(
				MessageType.CLIENT_CLOSES_CONNECTION, clientAddress, null));
		if (inSocket != null) {
			context.destroySocket(inSocket);
			inSocket = null;
			System.out.println("Connection to master closed.");
		}
	}

	@Override
	public void close() {
		if (outSocket != null) {
			closeConnectionToMaster();
			context.destroySocket(outSocket);
			NetworkContextFactory.destroyNetworkContext(context);
			outSocket = null;
		}
	}

}
