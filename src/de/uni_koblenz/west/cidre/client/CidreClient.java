package de.uni_koblenz.west.cidre.client;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.logger.JeromqStreamHandler;

public class CidreClient {

	private final ClientConnection connection;

	public CidreClient() {
		connection = new ClientConnection();
	}

	public void startUp(String masterAddress) {
		connection.connect(masterAddress);
		System.out.println(getClass().getSimpleName() + " started.");
	}

	public void processCommands() {
		// TODO Auto-generated method stub

	}

	public void shutDown() {
		connection.close();
		System.out.println(getClass().getSimpleName() + " stopped");
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

			CidreClient client = new CidreClient();
			client.startUp(master);
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
