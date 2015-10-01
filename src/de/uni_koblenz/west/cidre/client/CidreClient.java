package de.uni_koblenz.west.cidre.client;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.logger.JeromqStreamHandler;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.messages.MessageUtils;
import de.uni_koblenz.west.cidre.common.networManager.NetworkContextFactory;

public class CidreClient {

	private final ZContext context;

	private Socket masterSocket;

	private Socket clientConnection;

	private String clientAddress;

	public CidreClient(String masterAddress) {
		context = NetworkContextFactory.getNetworkContext();
		masterSocket = context.createSocket(ZMQ.PUSH);
		masterSocket.connect("tcp://" + masterAddress);
		System.out.println(getClass().getName() + " started.");
	}

	public void connect() {
		System.out.println("Connecting to master...");
		if (clientConnection == null) {
			clientConnection = context.createSocket(ZMQ.PULL);
			try {
				String hostAddress = InetAddress.getLocalHost()
						.getHostAddress();
				int port = clientConnection.bindToRandomPort(hostAddress, 49152,
						61000);
				clientAddress = hostAddress + ":" + port;

				// exchange a unique connection with master
				masterSocket.send(MessageUtils.createStringMessage(
						MessageType.CLIENT_CONNECTION_CREATION, clientAddress,
						null));
				clientConnection.setReceiveTimeOut(
						(int) Configuration.CLIENT_CONNECTION_TIMEOUT);
				byte[] answer = clientConnection.recv();
				if (answer == null
						|| (answer.length != 1 && MessageType.valueOf(
								answer[0]) != MessageType.CLIENT_CONNECTION_CONFIRMATION)) {
					System.out.println(
							"Master is not confirming connection attempt.");
					closeConnectionToMaster();
				}
				new Thread() {
					@Override
					public void run() {
						while (!isInterrupted() && clientConnection != null) {
							long startTime = System.currentTimeMillis();
							clientConnection.send(new byte[] {
									MessageType.CLIENT_IS_ALIVE.getValue() });
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
				shutDown();
				throw new RuntimeException(e);
			}
		}
		System.out.println("Connection established.");
	}

	public void processCommands() {
		// TODO Auto-generated method stub

	}

	private void closeConnectionToMaster() {
		masterSocket.send(MessageUtils.createStringMessage(
				MessageType.CLIENT_CLOSES_CONNECTION, clientAddress, null));
		if (clientConnection != null) {
			clientConnection.close();
			clientConnection = null;
		}
		System.out.println("Connection to master closed.");
	}

	public void shutDown() {
		if (masterSocket != null) {
			closeConnectionToMaster();
			masterSocket.close();
			NetworkContextFactory.destroyNetworkContext(context);
			System.out.println(getClass().getName() + " stopped");
			masterSocket = null;
		}
	}

	public static void main(String[] args) {
		Options options = createCommandLineOptions();
		try {
			CommandLine line = parseCommandLineArgs(options, args);
			if (line.hasOption("h")) {
				printUsage(options);
				return;
			}
			String master = JeromqStreamHandler.DEFAULT_PORT;
			if (line.hasOption("m")) {
				master = line.getOptionValue("m");
			}

			if (master.indexOf(':') == -1) {
				master += ":" + Configuration.DEFAULT_CLIENT_PORT;
			}

			CidreClient client = new CidreClient(master);
			client.connect();
			client.processCommands();

			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
				@Override
				public void run() {
					client.shutDown();
				}
			}));

		} catch (ParseException e) {
			e.printStackTrace();
			printUsage(options);
		}
	}

	private static Options createCommandLineOptions() {
		Option help = new Option("h", "help", false, "print this help message");
		help.setRequired(false);

		Option master = Option.builder("m").longOpt("master").hasArg()
				.argName("IP:Port")
				.desc("IP and address of CIDRE master. If no port is specified, port "
						+ Configuration.DEFAULT_CLIENT_PORT
						+ " is used as default.")
				.required(true).build();

		Options options = new Options();
		options.addOption(help);
		options.addOption(master);
		return options;
	}

	private static CommandLine parseCommandLineArgs(Options options,
			String[] args) throws ParseException {
		CommandLineParser parser = new DefaultParser();
		return parser.parse(options, args);
	}

	private static void printUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(
				"java " + CidreClient.class.getName() + " [-h] [-m <IP:Port>] ",
				options);
	}

}
