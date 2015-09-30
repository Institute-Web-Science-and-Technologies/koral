package de.uni_koblenz.west.cidre.common.system;

import java.io.Closeable;
import java.io.IOException;
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

public abstract class CidreSystem extends Thread {

	protected Logger logger;

	private final NetworkManager networkManager;

	public CidreSystem(Configuration conf, String[] currentAddress) {
		// add shutdown hook that terminates everything
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				if (isAlive()) {
					interrupt();
				}
			}
		}));

		if (conf.getLoglevel() != Level.OFF) {
			if (conf.getRomoteLoggerReceiver() != null) {
				logger = LoggerFactory.getJeromqLogger(conf, currentAddress,
						getClass().getName(), conf.getRomoteLoggerReceiver());
			}
			try {
				logger = LoggerFactory.getCSVFileLogger(conf, currentAddress,
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

		networkManager = new NetworkManager(conf, logger, currentAddress);

		if (logger != null) {
			logger.info(getClass().getSimpleName() + " started");
		}
	}

	protected NetworkManager getNetworkManager() {
		return networkManager;
	}

	@Override
	public void run() {
		while (!isInterrupted()) {
			runOneIteration();
		}
		shutDown();
	}

	protected abstract void runOneIteration();

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
		shutDownInternal();

		if (logger != null) {
			logger.info(getClass().getSimpleName() + " shutted down");
		}
	}

	protected abstract void shutDownInternal();

	protected static Options createCommandLineOptions() {
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

	protected static CommandLine parseCommandLineArgs(Options options,
			String[] args) throws ParseException {
		CommandLineParser parser = new DefaultParser();
		return parser.parse(options, args);
	}

	protected static Configuration initializeConfiguration(Options options,
			CommandLine line, String className, String additionalArgs) {
		if (line.hasOption("h")) {
			printUsage(className, options, additionalArgs);
			return null;
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

		return conf;
	}

	protected static void printUsage(String className, Options options,
			String additionalArgs) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("java " + className
				+ " [-h] [-c <configFile>] [-r <receiverIP:Port>] "
				+ additionalArgs, options);
	}

}
