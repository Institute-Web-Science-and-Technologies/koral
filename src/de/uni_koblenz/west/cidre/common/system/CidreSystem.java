package de.uni_koblenz.west.cidre.common.system;

import java.io.IOException;
import java.util.HashMap;
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
import de.uni_koblenz.west.cidre.common.executor.WorkerManager;
import de.uni_koblenz.west.cidre.common.logger.JeromqStreamHandler;
import de.uni_koblenz.west.cidre.common.logger.LoggerFactory;
import de.uni_koblenz.west.cidre.common.messages.MessageListener;
import de.uni_koblenz.west.cidre.common.messages.MessageNotifier;
import de.uni_koblenz.west.cidre.common.networManager.NetworkManager;

/**
 * This class abstracts the functionality that is the same for the CIDRE master
 * and any slave, for instance, the message passing facility or the clean shut
 * down.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public abstract class CidreSystem extends Thread implements MessageNotifier {

	protected Logger logger;

	private final NetworkManager networkManager;

	private final WorkerManager workerManager;

	/**
	 * Only listens on messages only from slaves! first slave has array index 0!
	 */
	private Map<Class<? extends MessageListener>, MessageListener[][]> listeners;

	public CidreSystem(Configuration conf, String[] currentAddress,
			NetworkManager networkManager) {
		// add shutdown hook that terminates everything
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				if (isAlive()) {
					interrupt();
				}
				try {
					join();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
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

		workerManager = new WorkerManager(conf, this, getNetworkManager(),
				getNetworkManager().getNumberOfSlaves(), logger);

		if (logger != null) {
			logger.info(getClass().getSimpleName() + " started");
		}
	}

	protected NetworkManager getNetworkManager() {
		return networkManager;
	}

	protected WorkerManager getWorkerManager() {
		return workerManager;
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
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			throw t;
		} finally {
			try {
				shutDown();
			} catch (Throwable t) {
				if (logger != null) {
					logger.throwing(t.getStackTrace()[0].getClassName(),
							t.getStackTrace()[0].getMethodName(), t);
				}
				throw t;
			}
		}
	}

	protected abstract void runOneIteration();

	@Override
	public void registerMessageListener(
			Class<? extends MessageListener> listenerType,
			MessageListener listener) {
		MessageListener[][] messageListeners = listeners.get(listenerType);
		if (messageListeners == null) {
			messageListeners = new MessageListener[networkManager
					.getNumberOfSlaves()][];
			listeners.put(listenerType, messageListeners);
		}
		int slaveID = listener.getSlaveID();
		if (slaveID == Integer.MAX_VALUE) {
			// register for listening on all slaves
			for (int i = 0; i < messageListeners.length; i++) {
				putListener(listener, messageListeners, i);
			}
		} else {
			slaveID--;
			putListener(listener, messageListeners, slaveID);
		}
	}

	/**
	 * 
	 * @param listener
	 * @param messageListeners
	 * @param slaveIndex
	 *            first slave has index 0!
	 */
	private void putListener(MessageListener listener,
			MessageListener[][] messageListeners, int slaveIndex) {
		if (messageListeners[slaveIndex] == null) {
			messageListeners[slaveIndex] = new MessageListener[1];
		}
		// find first free index
		int insertIndex = 0;
		while (insertIndex < messageListeners[slaveIndex].length
				&& messageListeners[slaveIndex][insertIndex] != null) {
			insertIndex++;
		}
		if (insertIndex == messageListeners[slaveIndex].length) {
			// extend array
			MessageListener[] newArray = new MessageListener[messageListeners[slaveIndex].length
					+ 1];
			System.arraycopy(messageListeners[slaveIndex], 0, newArray, 0,
					messageListeners[slaveIndex].length);
			messageListeners[slaveIndex] = newArray;
		}
		messageListeners[slaveIndex][insertIndex] = listener;
	}

	@Override
	public void notifyMessageListener(
			Class<? extends MessageListener> listenerType, int slaveID,
			byte[][] message) {
		MessageListener[][] messageListeners = listeners.get(listenerType);
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
		for (MessageListener listener : messageListeners[slaveID]) {
			if (listener != null) {
				listener.processMessage(message);
			}
		}
	}

	@Override
	public void notifyMessageListener(
			Class<? extends MessageListener> listenerType, int slaveID,
			byte[] message) {
		MessageListener[][] messageListeners = listeners.get(listenerType);
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
		for (MessageListener listener : messageListeners[slaveID]) {
			if (listener != null) {
				listener.processMessage(message);
			}
		}
	}

	@Override
	public void unregisterMessageListener(
			Class<? extends MessageListener> listenerType,
			MessageListener listener) {
		MessageListener[][] messageListeners = listeners.get(listenerType);
		if (messageListeners == null) {
			return;
		}
		int slaveID = listener.getSlaveID();
		if (slaveID == Integer.MAX_VALUE) {
			for (int i = 0; i < messageListeners.length; i++) {
				if (messageListeners[i] != null) {
					removeListener(listener, messageListeners, i);
				}
			}
		} else {
			slaveID--;
			if (messageListeners[slaveID] == null) {
				return;
			}
			removeListener(listener, messageListeners, slaveID);
		}
		for (MessageListener[] list : messageListeners) {
			if (list != null) {
				return;
			}
		}
		// there are no registered listeners of this type any more
		listeners.remove(listenerType);
	}

	/**
	 * 
	 * @param listener
	 * @param messageListeners
	 * @param slaveIndex
	 *            first slave has index 0!
	 */
	private void removeListener(MessageListener listener,
			MessageListener[][] messageListeners, int slaveIndex) {
		boolean containsElement = false;
		for (int i = 0; i < messageListeners[slaveIndex].length; i++) {
			if (messageListeners[slaveIndex][i] == listener) {
				messageListeners[slaveIndex][i] = null;
			}
			containsElement |= messageListeners[slaveIndex][i] != null;
		}
		if (!containsElement) {
			messageListeners[slaveIndex] = null;
		}
	}

	public void shutDown() {
		shutDownInternal();
		workerManager.close();
		networkManager.close();
	}

	protected abstract void shutDownInternal();

	public void clear() {
		for (MessageListener[][] value : listeners.values()) {
			if (value != null) {
				for (MessageListener[] list : value) {
					if (list != null) {
						for (MessageListener listener : list) {
							if (listener != null) {
								listener.close();
							}
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
