package de.uni_koblenz.west.cidre.client;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.logger.JeromqStreamHandler;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.master.client_manager.FileChunk;
import de.uni_koblenz.west.cidre.master.graph_cover_creator.CoverStrategyType;

public class CidreClient {

	private final ClientConnection connection;

	public CidreClient() {
		connection = new ClientConnection();
	}

	public void startUp(String masterAddress) {
		connection.connect(masterAddress);
	}

	public void loadGraph(CoverStrategyType graphCover,
			int nHopReplicationPathLength, String... inputPaths) {
		List<File> files = getFiles(inputPaths);
		if (files.isEmpty()) {
			throw new RuntimeException("No graph file could be found.");
		}
		byte[][] args = new byte[4][];
		args[0] = new byte[] { (byte) 3 };
		args[1] = ByteBuffer.allocate(4).putInt(graphCover.ordinal()).array();
		args[2] = ByteBuffer.allocate(4).putInt(nHopReplicationPathLength)
				.array();
		args[3] = ByteBuffer.allocate(4).putInt(files.size()).array();
		connection.sendCommand("load", args);

		byte[][] response = connection.getResponse();
		try (FileSetChunkReader reader = new FileSetChunkReader();) {
			while (response != null) {
				MessageType mtype = MessageType.valueOf(response[0][0]);
				if (mtype == MessageType.REQUEST_FILE_CHUNK) {
					int fileID = ByteBuffer.wrap(response[0], 1, 4).getInt();
					long chunkID = ByteBuffer.wrap(response[0], 5, 8).getLong();
					FileChunk fileChunk = reader.getFileChunk(files.get(fileID),
							fileID, chunkID);
					connection.sendFileChunk(fileChunk);
					// some output for user
					if (fileChunk.getSequenceNumber() == 0) {
						System.out.println("Sending file " + files
								.get(fileChunk.getFileID()).getAbsolutePath());
					} else {
						final long numberOfOutputs = 10;
						long outputInterval = fileChunk
								.getTotalNumberOfSequences() / numberOfOutputs;
						if (fileChunk.getSequenceNumber()
								% outputInterval == 0) {
							System.out.println((fileChunk.getSequenceNumber()
									/ outputInterval * numberOfOutputs)
									+ " % finished");
						}
					}
				} else {
					processCommandResponse("loading of a graph", response);
					break;
				}
				response = connection.getResponse();
			}
		} catch (Throwable t) {
			connection.sendCommandAbortion("load");
			throw t;
		}
		if (response == null) {
			System.out.println("loading of a graph failed");
		}
	}

	private void processCommandResponse(String individualMessage,
			byte[][] response) {
		try {
			MessageType mtype = MessageType.valueOf(response[0][0]);
			switch (mtype) {
			case CLIENT_COMMAND_SUCCEEDED:
				System.out.println(
						individualMessage + " has finished successfully");
				break;
			case CLIENT_COMMAND_FAILED:
				System.out.println(individualMessage + " has failed.");
				try {
					System.out.println("Cause: " + new String(response[0], 1,
							response[0].length - 1, "UTF-8"));
				} catch (UnsupportedEncodingException e) {
					throw new RuntimeException(e);
				}
				break;
			default:
				throw new RuntimeException(
						"Unexpected message type " + mtype.name());
			}
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(
					"Unknwon message type " + response[0][0]);
		}
	}

	private List<File> getFiles(String[] inputPaths) {
		List<File> files = new ArrayList<>();
		for (String inputPath : inputPaths) {
			File file = new File(inputPath);
			if (!file.exists()) {
				continue;
			}
			if (file.isFile()) {
				files.add(file);
			} else {
				for (File containedFile : file.listFiles()) {
					if (containedFile.isFile()) {
						files.add(containedFile);
					}
				}
			}
		}
		return files;
	}

	public void shutDown() {
		connection.close();
		System.out.println(getClass().getSimpleName() + " stopped");
	}

	public static void main(String[] args) {
		String[][] argParts = splitArgs(args);
		Options options = createCommandLineOptions();
		try {
			CommandLine line = parseCommandLineArgs(options, argParts[0]);
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

			CidreClient client = new CidreClient();
			client.startUp(master);

			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
				@Override
				public void run() {
					client.shutDown();
				}
			}));

			if (argParts[1].length > 0) {
				executeCommand(client, argParts[1]);
			} else {
				startCLI(client);
			}

		} catch (ParseException e) {
			e.printStackTrace();
			printUsage(options);
		}
	}

	private static void startCLI(CidreClient client) {
		System.out.println("Client ready for receiving commands");
		System.out.println("For help enter \"help\".");
		System.out.println(
				"If you want to stop the client enter \"exit\" or \"quit\".");
		try (Scanner scanner = new Scanner(System.in);) {
			while (true) {
				System.out.print("> ");
				if (scanner.hasNext()) {
					String line = scanner.nextLine().trim();
					if (!line.isEmpty()) {
						String[] command = line.split("\\s+");
						if (command.length == 1 && (command[0].toLowerCase()
								.equals("exit")
								|| command[0].toLowerCase().equals("quit"))) {
							break;
						}
						try {
							executeCommand(client, command);
						} catch (RuntimeException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
	}

	private static void executeCommand(CidreClient client, String[] strings) {
		String[] args = new String[strings.length - 1];
		if (args.length > 0) {
			System.arraycopy(strings, 1, args, 0, args.length);
		}
		try {
			switch (strings[0].toLowerCase()) {
			case "help":
				printCommandList();
				break;
			case "load":
				loadGraph(client, args);
				break;
			}

		} catch (ParseException e) {
			System.out.println("Invalid command syntax.");
			printCommandList();
		}
	}

	private static void loadGraph(CidreClient client, String[] args)
			throws ParseException {
		Options options = createLoadOptions();
		CommandLineParser parser = new DefaultParser();
		CommandLine commandLine = parser.parse(options, args);

		CoverStrategyType graphCover = CoverStrategyType
				.valueOf(commandLine.getOptionValue("c"));

		int nHopReplicationPathLength = 0;
		if (commandLine.hasOption("n")) {
			nHopReplicationPathLength = Integer
					.parseInt(commandLine.getOptionValue("n"));
		}

		List<String> inputPaths = commandLine.getArgList();
		if (inputPaths.isEmpty()) {
			throw new ParseException(
					"Please specify at least one graph file to load.");
		}
		client.loadGraph(graphCover, nHopReplicationPathLength,
				inputPaths.toArray(new String[inputPaths.size()]));
	}

	private static Options createLoadOptions() {
		StringBuilder sb = new StringBuilder();
		String delim = "";
		for (CoverStrategyType type : CoverStrategyType.values()) {
			sb.append(delim).append(type.name());
			delim = ", ";
		}

		Option coverStrategy = Option.builder("c").longOpt("cover").hasArg()
				.argName("graphCoverStrategy")
				.desc("The used graph cover strategy wher <graphCoverStrategy> is one of "
						+ sb.toString())
				.required(true).build();

		Option nHopReplication = Option.builder("n").longOpt("nHopReplication")
				.hasArg().argName("path length")
				.desc("Performs an n-hop replication on the chosen graph cover strategy.")
				.required(false).build();

		Options options = new Options();
		options.addOption(coverStrategy);
		options.addOption(nHopReplication);
		return options;
	}

	private static String[][] splitArgs(String[] args) {
		String[][] parts = new String[2][];
		int i = 0;
		for (i = 0; i < args.length; i++) {
			if (isCommand(args[i])) {
				parts[0] = new String[i];
				System.arraycopy(args, 0, parts[0], 0, parts[0].length);
				parts[1] = new String[args.length - i];
				System.arraycopy(args, i, parts[1], 0, parts[1].length);
				break;
			}
		}
		if (i >= args.length) {
			// there does not exist a command
			// there are only general arguments
			parts[0] = args;
			parts[1] = new String[0];
		}
		return parts;
	}

	private static boolean isCommand(String string) {
		switch (string.toLowerCase()) {
		case "help":
		case "exit":
		case "quit":
		case "load":
			return true;
		default:
			return false;
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
		formatter.printHelp("java " + CidreClient.class.getName()
				+ " [-h] [-m <IP:Port>] <command>", options);
		System.out.println("The following commands are available:");
		printCommandList();
	}

	private static void printCommandList() {
		System.out.println("help\tprints this help message");
		System.out.println("exit\tquits the client");
		System.out.println("quit\tquits the client");
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(
				"load -c <graphCoverStrategy> [-n <pathLength>] <fileOrFolder>...",
				createLoadOptions());
	}

}
