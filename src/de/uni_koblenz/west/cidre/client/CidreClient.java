package de.uni_koblenz.west.cidre.client;

import java.util.Arrays;
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

public class CidreClient {

	private final ClientConnection connection;

	public CidreClient() {
		connection = new ClientConnection();
	}

	public void startUp(String masterAddress) {
		connection.connect(masterAddress);
	}

	// TODO Auto-generated method stub

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
				executeCommand(argParts[1]);
			} else {
				startCLI();
			}

		} catch (ParseException e) {
			e.printStackTrace();
			printUsage(options);
		}
	}

	private static void startCLI() {
		System.out.println("Client ready for receiving commands");
		System.out.println("For help enter \"help\".");
		System.out.println("If you want to stop the client enter \"exit\".");
		try (Scanner scanner = new Scanner(System.in);) {
			while (true) {
				System.out.print("> ");
				if (scanner.hasNext()) {
					String line = scanner.nextLine().trim();
					if (!line.isEmpty()) {
						String[] command = line.split("\\s+");
						if (command.length == 1
								&& command[0].toLowerCase().equals("exit")) {
							break;
						}
						executeCommand(command);
					}
				}
			}
		}
	}

	private static void executeCommand(String[] strings) {
		System.out.println(Arrays.toString(strings));
		// TODO Auto-generated method stub

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
		formatter.printHelp(
				"java " + CidreClient.class.getName() + " [-h] [-m <IP:Port>] ",
				options);
	}

}
