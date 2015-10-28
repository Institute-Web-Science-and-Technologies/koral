package de.uni_koblenz.west.cidre.common.system;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
import de.uni_koblenz.west.cidre.common.networManager.MessageListener;
import de.uni_koblenz.west.cidre.common.networManager.MessageNotifier;
import de.uni_koblenz.west.cidre.common.networManager.NetworkManager;

public abstract class CidreSystem extends Thread implements MessageNotifier {

	protected Logger logger;

	private final NetworkManager networkManager;

	private Map<Class<? extends MessageListener>, List<? extends MessageListener>[]> listeners;

	public CidreSystem(Configuration conf, String[] currentAddress,
			NetworkManager networkManager) {
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

		this.networkManager = networkManager;

		listeners = new HashMap<>();

		if (logger != null) {
			logger.info(getClass().getSimpleName() + " started");
		}
	}

	protected NetworkManager getNetworkManager() {
		return networkManager;
	}

	@Override
	public void run() {
		try {
			while (!isInterrupted()) {
				runOneIteration();
			}
			if (logger != null) {
				logger.info(getClass().getSimpleName() + " shutted down");
			}
		} catch (Throwable t) {
			if (logger != null) {
				logger.throwing(t.getStackTrace()[0].getClassName(),
						t.getStackTrace()[0].getMethodName(), t);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}
			}
			throw t;
		} finally {
			shutDown();
		}
	}

	protected abstract void runOneIteration();

	@Override
	@SuppressWarnings("unchecked")
	public <V extends MessageListener> void registerMessageListener(
			Class<V> listenerType, V listener) {
		List<V>[] messageListeners = (List<V>[]) listeners.get(listenerType);
		if (messageListeners == null) {
			messageListeners = new List[networkManager.getNumberOfSlaves()];
			listeners.put(listenerType, messageListeners);
		}
		int slaveID = listener.getSlaveID() - 1;
		if (messageListeners[slaveID] == null) {
			messageListeners[slaveID] = new LinkedList<>();
		}
		messageListeners[slaveID].add(listener);
	}

	@Override
	public <V extends MessageListener> void notifyMessageListener(
			Class<V> listenerType, int slaveID, byte[][] message) {
		@SuppressWarnings("unchecked")
		List<V>[] messageListeners = (List<V>[]) listeners.get(listenerType);
		if (messageListeners == null) {
			if (logger != null) {
				logger.finer(
						"No message listners of type " + listenerType.getName()
								+ " registered. Discarding message.");
			}
			return;
		}
		slaveID--;
		if (messageListeners[slaveID] == null) {
			if (logger != null) {
				logger.finer(
						"No message listners of type " + listenerType.getName()
								+ " registered for slave. Discarding message.");
			}
			return;
		}
		for (V listener : messageListeners[slaveID]) {
			listener.processMessage(message);
		}
	}

	@Override
	public <V extends MessageListener> void unregisterMessageListener(
			Class<V> listenerType, V listener) {
		@SuppressWarnings("unchecked")
		List<V>[] messageListeners = (List<V>[]) listeners.get(listenerType);
		if (messageListeners == null) {
			return;
		}
		int slaveID = listener.getSlaveID() - 1;
		if (messageListeners[slaveID] == null) {
			return;
		}
		messageListeners[slaveID].remove(listener);
		if (messageListeners[slaveID].isEmpty()) {
			messageListeners[slaveID] = null;
		}
		for (List<V> list : messageListeners) {
			if (list != null) {
				return;
			}
		}
		// there are no registered listerners of this type any more
		listeners.remove(listenerType);
	}

	public void shutDown() {
		networkManager.close();
		shutDownInternal();
	}

	protected abstract void shutDownInternal();

	public void clear() {
		for (List<? extends MessageListener>[] value : listeners.values()) {
			if (value != null) {
				for (List<? extends MessageListener> list : value) {
					if (list != null) {
						for (MessageListener listener : list) {
							listener.close();
						}
					}
				}
			}
		}
		listeners = new HashMap<>();
		clearInternal();
	}

	public abstract void clearInternal();

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
