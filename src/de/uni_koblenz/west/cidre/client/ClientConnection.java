package de.uni_koblenz.west.cidre.client;

import java.io.Closeable;
import java.io.UnsupportedEncodingException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Enumeration;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.messages.MessageUtils;
import de.uni_koblenz.west.cidre.common.networManager.NetworkContextFactory;
import de.uni_koblenz.west.cidre.master.client_manager.FileChunk;

public class ClientConnection implements Closeable {

	private final ZContext context;

	private Socket outSocket;

	private Socket inSocket;

	private String clientAddress;

	public ClientConnection() {
		context = NetworkContextFactory.getNetworkContext();
	}

	public String getClientAddress() {
		return clientAddress;
	}

	public void connect(String masterAddress) {
		System.out.println("Connecting to master...");
		outSocket = context.createSocket(ZMQ.PUSH);
		outSocket.connect("tcp://" + masterAddress);
		if (inSocket == null) {
			inSocket = context.createSocket(ZMQ.PULL);
			inSocket.setReceiveTimeOut(
					(int) Configuration.CLIENT_CONNECTION_TIMEOUT);
			try {
				String hostAddress = getHostAddress();
				int port = inSocket.bindToRandomPort("tcp://" + hostAddress,
						49152, 61000);
				clientAddress = hostAddress + ":" + port;

				// exchange a unique connection with master
				outSocket.send(MessageUtils.createStringMessage(
						MessageType.CLIENT_CONNECTION_CREATION, clientAddress,
						null));
				byte[] answer = inSocket.recv();
				if (answer == null
						|| (answer.length != 1 && MessageType.valueOf(
								answer[0]) != MessageType.CLIENT_CONNECTION_CONFIRMATION)) {
					System.out.println(
							"Master is not confirming connection attempt.");
					closeConnectionToMaster();
					return;
				}
				Thread keepAliveThread = new Thread() {
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
				};
				keepAliveThread.setDaemon(true);
				keepAliveThread.start();
			} catch (UnknownHostException e) {
				System.out.println(
						"Connection failed because the local IP address could not be identified.");
				closeConnectionToMaster();
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

	public boolean isConnected() {
		return inSocket != null;
	}

	public void sendCommand(String command, byte[][] args) {
		if (!isConnected()) {
			throw new RuntimeException(
					"The client has not connected to the master, yet.");
		}
		try {
			byte[] clientAddress = this.clientAddress.getBytes("UTF-8");
			byte[] commandBytes = command.getBytes("UTF-8");

			outSocket.sendMore(
					new byte[] { MessageType.CLIENT_COMMAND.getValue() });
			outSocket.sendMore(clientAddress);
			outSocket.sendMore(commandBytes);
			if (args.length == 0) {
				outSocket.send(new byte[] { (byte) 0 });
			} else {
				for (int i = 0; i < args.length; i++) {
					if (i == args.length - 1) {
						outSocket.send(args[i]);
					} else {
						outSocket.sendMore(args[i]);
					}
				}
			}
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	public void sendFileChunk(FileChunk fileChunk) {
		if (!isConnected()) {
			throw new RuntimeException(
					"The client has not connected to the master, yet.");
		}
		try {
			byte[] clientAddress = this.clientAddress.getBytes("UTF-8");

			outSocket
					.sendMore(new byte[] { MessageType.FILE_CHUNK.getValue() });
			outSocket.sendMore(clientAddress);
			outSocket.sendMore(ByteBuffer.allocate(4)
					.putInt(fileChunk.getFileID()).array());
			outSocket.sendMore(ByteBuffer.allocate(8)
					.putLong(fileChunk.getSequenceNumber()).array());
			outSocket.sendMore(ByteBuffer.allocate(8)
					.putLong(fileChunk.getTotalNumberOfSequences()).array());
			outSocket.send(fileChunk.getContent());
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	public byte[][] getResponse() {
		if (!isConnected()) {
			throw new RuntimeException(
					"The client has not connected to the master, yet.");
		}
		byte[][] response = null;
		long startTime = System.currentTimeMillis();
		byte[] mType = null;
		while (mType == null && System.currentTimeMillis()
				- startTime < Configuration.CLIENT_CONNECTION_TIMEOUT) {
			mType = inSocket.recv(ZMQ.DONTWAIT);
			if (mType == null) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
				}
			}
		}
		try {
			if (mType == null) {
				System.out.println("Master did not respond to request.");
				return null;
			}
			MessageType messageType = MessageType.valueOf(mType[0]);
			switch (messageType) {
			case REQUEST_FILE_CHUNK:
			case MASTER_WORK_IN_PROGRESS:
			case CLIENT_COMMAND_SUCCEEDED:
			case CLIENT_COMMAND_FAILED:
				response = new byte[1][];
				break;
			default:
				throw new RuntimeException("Unexpected response from server: "
						+ messageType.name());
			}
			response[0] = mType;
			for (int i = 1; i < response.length; i++) {
				response[i] = inSocket.recv();
			}
		} catch (IllegalArgumentException e) {
			throw new RuntimeException("Unknwon message type " + mType[0], e);
		}
		return response;
	}

	public void sendCommandAbortion(String command) {
		outSocket.send(MessageUtils.createStringMessage(
				MessageType.CLIENT_COMMAND_ABORTED,
				clientAddress + "|" + command, null));
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
