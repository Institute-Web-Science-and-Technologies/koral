package de.uni_koblenz.west.cidre.master;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.config.impl.XMLDeserializer;
import de.uni_koblenz.west.cidre.common.logger.JeromqStreamHandler;
import de.uni_koblenz.west.cidre.common.logger.LoggerFactory;
import de.uni_koblenz.west.cidre.common.networManager.NetworkManager;

public class CidreMaster extends Thread {

	private Logger logger;

	private final NetworkManager networkManager;

	public CidreMaster(Configuration conf) {
		if (conf.getLoglevel() != Level.OFF) {
			if (conf.getRomoteLoggerReceiver() != null) {
				logger = LoggerFactory.getJeromqLogger(conf, conf.getMaster(),
						getClass().getName(), conf.getRomoteLoggerReceiver());
			}
			try {
				logger = LoggerFactory.getCSVFileLogger(conf, conf.getMaster(),
						getClass().getName());
			} catch (IOException e) {
				if (logger != null) {
					logger.warning(
							"Logging to a CSV file is not possible. Reason: "
									+ e.getMessage());
					logger.warning("Continuing without logging to a file.");
					logger.throwing(e.getStackTrace()[0].getClassName(),
							e.getStackTrace()[0].getMethodName(), e);
				}
				e.printStackTrace();
			}
		}

		networkManager = new NetworkManager(conf, logger, conf.getMaster());

		Arrays.toString(logger.getHandlers());

		if (logger != null) {
			logger.info("CIDRE master started");
		}
	}

	@Override
	public void run() {
		while (!isInterrupted()) {
			// TODO
			break;
		}
		shutDown();
	}

	public void shutDown() {
		networkManager.close();
		for (Handler handler : logger.getHandlers()) {
			if (handler instanceof Closeable) {
				try {
					((Closeable) handler).close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
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
			String confFile = "cidreConfig.xml";
			if (line.hasOption("c")) {
				confFile = line.getOptionValue("c");
			}
			Configuration conf = new Configuration();
			new XMLDeserializer().deserialize(conf, confFile);

			if (line.hasOption("r")) {
				conf.setRomoteLoggerReceiver(line.getOptionValue("r"));
			}

			CidreMaster master = new CidreMaster(conf);
			master.start();

			// add shutdown hook that terminates everything
			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
				@Override
				public void run() {
					master.interrupt();
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

		Option config = Option.builder("c").longOpt("config").hasArg()
				.argName("configFile")
				.desc("the configuration file to use. default is ./cidreConfig.xml")
				.required(false).build();

		Option remoteLogger = Option.builder("r").longOpt("remoteLogger")
				.hasArg().argName("receiverIP:Port")
				.desc("remote receiver to which logging messages are sent. If no port is specified, port "
						+ JeromqStreamHandler.DEFAULT_PORT
						+ " is used as default.")
				.required(false).build();

		Options options = new Options();
		options.addOption(help);
		options.addOption(config);
		options.addOption(remoteLogger);
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
				"java " + CidreMaster.class.getName()
						+ " [-h] [-c <configFile>] [-r <receiverIP:Port>] ",
				options);
	}

}
