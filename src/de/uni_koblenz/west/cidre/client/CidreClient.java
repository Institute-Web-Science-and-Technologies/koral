package de.uni_koblenz.west.cidre.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
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
import org.apache.jena.query.QueryFactory;
import org.apache.jena.util.FileUtils;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.fileTransfer.FileChunk;
import de.uni_koblenz.west.cidre.common.fileTransfer.FileSender;
import de.uni_koblenz.west.cidre.common.logger.JeromqStreamHandler;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.messages.MessageUtils;
import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.cidre.common.query.parser.QueryExecutionTreeType;
import de.uni_koblenz.west.cidre.common.query.parser.SparqlParser;
import de.uni_koblenz.west.cidre.common.query.parser.VariableDictionary;
import de.uni_koblenz.west.cidre.common.utils.GraphFileFilter;
import de.uni_koblenz.west.cidre.common.utils.NumberConversion;
import de.uni_koblenz.west.cidre.master.graph_cover_creator.CoverStrategyType;

/**
 * API and command line interface to interact with CIDRE master.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class CidreClient {

	private final ClientConnection connection;

	public CidreClient() {
		connection = new ClientConnection();
	}

	public boolean startUp(String masterAddress) {
		connection.connect(masterAddress);
		return connection.isConnected();
	}

	public void loadGraph(CoverStrategyType graphCover,
			int nHopReplicationPathLength, String... inputPaths) {
		List<File> files = getFiles(inputPaths);
		if (files.isEmpty()) {
			throw new RuntimeException("No graph file could be found.");
		}
		byte[][] args = new byte[4 + files.size()][];
		args[0] = new byte[] { (byte) (args.length - 1) };
		args[1] = NumberConversion.int2bytes(graphCover.ordinal());
		args[2] = NumberConversion.int2bytes(nHopReplicationPathLength);
		args[3] = NumberConversion.int2bytes(files.size());
		fillWithFileEndings(args, 4, files);
		connection.sendCommand("load", args);

		byte[][] response = connection.getResponse();
		try (FileSender sender = new FileSender(files, connection);) {
			while (response != null) {
				MessageType mtype = MessageType.valueOf(response[0][0]);
				if (mtype == MessageType.FILE_CHUNK_REQUEST) {
					int fileID = NumberConversion.bytes2int(response[0], 1);
					long chunkID = NumberConversion.bytes2long(response[0], 5);
					FileChunk fileChunk = sender.sendFileChunk(0, fileID,
							chunkID);
					// some output for user
					if (fileChunk.getSequenceNumber() == 0) {
						System.out.println("Sending file " + files
								.get(fileChunk.getFileID()).getAbsolutePath());
					} else {
						final long numberOfOutputs = 10;
						long outputInterval = fileChunk
								.getTotalNumberOfSequences() / numberOfOutputs;
						if (outputInterval > 0 && fileChunk.getSequenceNumber()
								% outputInterval == 0) {
							System.out.println((fileChunk.getSequenceNumber()
									/ outputInterval * numberOfOutputs)
									+ "% finished");
						}
					}
				} else if (mtype == MessageType.MASTER_WORK_IN_PROGRESS) {
					if (response[0].length > 1) {
						System.out.println(MessageUtils
								.extractMessageString(response[0], null));
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

	private List<File> getFiles(String[] inputPaths) {
		GraphFileFilter filter = new GraphFileFilter();
		List<File> files = new ArrayList<>();
		for (String inputPath : inputPaths) {
			File file = new File(inputPath);
			if (!file.exists()) {
				continue;
			}
			if (file.isFile()) {
				if (filter.accept(file)) {
					files.add(file);
				}
			} else {
				for (File containedFile : file.listFiles(filter)) {
					if (containedFile.isFile()) {
						files.add(containedFile);
					}
				}
			}
		}
		return files;
	}

	private void fillWithFileEndings(byte[][] array, int startIndex,
			List<File> files) {
		for (int i = startIndex; i < array.length; i++) {
			String filename = files.get(i - startIndex).getAbsolutePath();
			String extension = "";
			if (filename.endsWith(".gz")) {
				extension = ".gz";
				filename = filename.substring(0, filename.length() - 3);
			}
			extension = FileUtils.getFilenameExt(filename) + extension;
			try {
				array[i] = extension.getBytes("UTF-8");
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public File processQueryFromFile(String queryFile, String outputFile,
			QueryExecutionTreeType treeType, boolean useBaseOperators)
					throws UnsupportedEncodingException, FileNotFoundException,
					IOException {
		return processQueryFromFile(new File(queryFile), outputFile, treeType,
				useBaseOperators);
	}

	public File processQueryFromFile(File queryFile, String outputFile,
			QueryExecutionTreeType treeType, boolean useBaseOperators)
					throws UnsupportedEncodingException, FileNotFoundException,
					IOException {
		return processQuery(readQueryFromFile(queryFile), outputFile, treeType,
				useBaseOperators);
	}

	public File processQuery(String query, String outputFile,
			QueryExecutionTreeType treeType, boolean useBaseOperators)
					throws UnsupportedEncodingException, FileNotFoundException,
					IOException {
		File output = new File(outputFile);
		try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(output), "UTF-8"));) {
			processQuery(query, bw, treeType, useBaseOperators);
		}
		return output;
	}

	public void processQueryFromFile(String queryFile, Writer outputWriter,
			QueryExecutionTreeType treeType, boolean useBaseOperators)
					throws FileNotFoundException, IOException {
		processQueryFromFile(new File(queryFile), outputWriter, treeType,
				useBaseOperators);
	}

	public void processQueryFromFile(File queryFile, Writer outputWriter,
			QueryExecutionTreeType treeType, boolean useBaseOperators)
					throws FileNotFoundException, IOException {
		processQuery(readQueryFromFile(queryFile), outputWriter, treeType,
				useBaseOperators);
	}

	public void processQuery(String query, Writer outputWriter,
			QueryExecutionTreeType treeType, boolean useBaseOperators)
					throws IOException {
		// check syntax
		VariableDictionary dictionary = new VariableDictionary();
		SparqlParser parser = new SparqlParser(null, null, (short) 0, 0, 0, 1,
				0, null, 0, 1, 1);
		QueryOperatorTask task = parser.parse(query, treeType, dictionary);
		String queryString = QueryFactory.create(query).serialize();
		String[] vars = dictionary.decode(task.getResultVariables());

		try {
			// send query
			byte[][] args = new byte[4][];
			args[0] = new byte[] { (byte) (args.length - 1) };
			args[1] = NumberConversion.int2bytes(treeType.ordinal());
			args[2] = new byte[] { useBaseOperators ? (byte) 1 : (byte) 0 };
			args[3] = queryString.getBytes("UTF-8");
			connection.sendCommand("query", args);

			// receive response
			try {
				byte[][] response = connection.getResponse();
				boolean isFirstResult = true;
				while (response != null) {
					MessageType mtype = MessageType.valueOf(response[0][0]);
					if (mtype == MessageType.MASTER_WORK_IN_PROGRESS) {
						if (response[0].length > 1) {
							System.out.println(MessageUtils
									.extractMessageString(response[0], null));
						}
					} else if (mtype == MessageType.QUERY_RESULT) {
						if (isFirstResult) {
							outputHeaders(vars, outputWriter);
							isFirstResult = false;
						}
						outputWriter.write(MessageUtils
								.convertToString(response[0], null));
						outputWriter.flush();
					} else {
						processCommandResponse("querying database", response);
						break;
					}
					response = connection.getResponse();
				}
			} catch (Throwable e) {
				connection.sendCommandAbortion("query");
				throw e;
			}
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	private void outputHeaders(String[] vars, Writer outputWriter)
			throws IOException {
		String delim = "";
		for (String var : vars) {
			outputWriter.write(delim);
			outputWriter.write(var);
			delim = Configuration.QUERY_RESULT_COLUMN_SEPARATOR_CHAR;
		}
	}

	private String readQueryFromFile(File queryFile)
			throws FileNotFoundException, IOException {
		try (BufferedReader br = new BufferedReader(
				new FileReader(queryFile));) {
			StringBuilder sb = new StringBuilder();
			String delim = "";
			for (String line = br.readLine(); line != null; line = br
					.readLine()) {
				sb.append(delim);
				sb.append(line);
				delim = "\n";
			}
			return sb.toString();
		}
	}

	public void dropDatabase() {
		connection.sendCommand("drop", new byte[0][0]);
		byte[][] response = connection.getResponse();
		while (response != null) {
			MessageType mtype = MessageType.valueOf(response[0][0]);
			if (mtype == MessageType.MASTER_WORK_IN_PROGRESS) {
				if (response[0].length > 1) {
					System.out.println(MessageUtils
							.extractMessageString(response[0], null));
				}
			} else {
				processCommandResponse("dropping database", response);
				break;
			}
			response = connection.getResponse();
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
			if (!client.startUp(master)) {
				client.shutDown();
				return;
			}

			Thread thisThread = Thread.currentThread();
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					client.shutDown();
					try {
						thisThread.join();
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				}
			});

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
			case "query":
				queryGraph(client, args);
				break;
			case "drop":
				dropDatabase(client, args);
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

	private static void queryGraph(CidreClient client, String[] args)
			throws ParseException {
		Options options = createQueryOptions();
		CommandLineParser parser = new DefaultParser();
		CommandLine commandLine = parser.parse(options, args);

		QueryExecutionTreeType treeType = QueryExecutionTreeType.LEFT_LINEAR;
		if (commandLine.hasOption("t")) {
			treeType = QueryExecutionTreeType
					.valueOf(commandLine.getOptionValue("t"));
		}

		boolean useBaselineOperators = commandLine.hasOption("b");

		try {
			if (commandLine.hasOption("q")) {
				if (commandLine.hasOption("o")) {
					File outputFile = new File(commandLine.getOptionValue("o"));
					System.out.println("Output written to "
							+ outputFile.getAbsolutePath());
					client.processQueryFromFile(commandLine.getOptionValue("q"),
							outputFile.getAbsolutePath(), treeType,
							useBaselineOperators);
				} else {
					System.out.println("Output written to console.");
					OutputStreamWriter writer = new OutputStreamWriter(
							System.out);
					client.processQueryFromFile(commandLine.getOptionValue("q"),
							writer, treeType, useBaselineOperators);
					writer.flush();
				}
			} else {
				List<String> inputQuery = commandLine.getArgList();
				if (inputQuery.isEmpty()) {
					throw new ParseException(
							"Please define the query to be requested.");
				}
				StringBuilder sb = new StringBuilder();
				String delim = "";
				for (String string : inputQuery) {
					sb.append(delim);
					sb.append(string);
					delim = " ";
				}
				if (commandLine.hasOption("o")) {
					File outputFile = new File(commandLine.getOptionValue("o"));
					System.out.println("Output written to "
							+ outputFile.getAbsolutePath());
					client.processQuery(sb.toString(),
							outputFile.getAbsolutePath(), treeType,
							useBaselineOperators);
				} else {
					System.out.println("Output written to console.");
					OutputStreamWriter writer = new OutputStreamWriter(
							System.out);
					client.processQuery(sb.toString(), writer, treeType,
							useBaselineOperators);
					writer.flush();
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static void dropDatabase(CidreClient client, String[] args) {
		client.dropDatabase();
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

	private static Options createQueryOptions() {
		StringBuilder sb = new StringBuilder();
		String delim = "";
		for (QueryExecutionTreeType type : QueryExecutionTreeType.values()) {
			sb.append(delim).append(type.name());
			delim = ", ";
		}

		Option treeType = Option.builder("t").longOpt("treeType").hasArg()
				.argName("treeType")
				.desc("The ordering in which the triple patterns of a BGP are joined. Valid options are "
						+ sb.toString() + ". The default value is "
						+ QueryExecutionTreeType.LEFT_LINEAR.name() + ".")
				.required(false).build();

		Option useBaseOperators = Option.builder("b").longOpt("base")
				.hasArg(false)
				.desc("If set, the baseline query operators are used.")
				.required(false).build();

		Option output = Option.builder("o").longOpt("output").hasArg()
				.argName("outputFile")
				.desc("The CSV file where the output is stored. If no file is given, the output is written to command line.")
				.required(false).build();

		Option queryFile = Option.builder("q").longOpt("querFile").hasArg()
				.argName("SPARQLQueryFile")
				.desc("A file with the single SPARQL query that should be executed.")
				.required(false).build();

		Options options = new Options();
		options.addOption(treeType);
		options.addOption(useBaseOperators);
		options.addOption(output);
		options.addOption(queryFile);
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
		case "query":
		case "drop":
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
		formatter.printHelp(
				"query [-t <treeType>] [-o <outputFile>] <SPARQL query>\n"
						+ "query [-t <treeType>] [-o <outputFile>] -q <SPARQLQueryFile>",
				createQueryOptions());
		System.out.println(
				"drop\tdeletes the database and its content. All running tasks, e.g., executed queries are terminated.");
	}

}
