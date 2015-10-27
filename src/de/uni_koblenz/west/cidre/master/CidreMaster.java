package de.uni_koblenz.west.cidre.master;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.system.CidreSystem;
import de.uni_koblenz.west.cidre.master.client_manager.ClientConnectionManager;
import de.uni_koblenz.west.cidre.master.client_manager.ClientMessageProcessor;
import de.uni_koblenz.west.cidre.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.cidre.master.statisticsDB.GraphStatistics;

public class CidreMaster extends CidreSystem {

	private final ClientMessageProcessor clientMessageProcessor;

	private final DictionaryEncoder dictionary;

	private final GraphStatistics statistics;

	private boolean graphHasBeenLoaded;

	public CidreMaster(Configuration conf) {
		super(conf, conf.getMaster());
		try {
			ClientConnectionManager clientConnections = new ClientConnectionManager(
					conf, logger);
			dictionary = new DictionaryEncoder(conf, logger);
			statistics = new GraphStatistics(conf,
					(short) conf.getNumberOfSlaves(), logger);
			clientMessageProcessor = new ClientMessageProcessor(conf,
					clientConnections, this, logger);
			graphHasBeenLoaded = false;
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
		}
	}

	public DictionaryEncoder getDictionary() {
		return dictionary;
	}

	public GraphStatistics getStatistics() {
		return statistics;
	}

	// TODO before sending to sparql reqester replace urn:blankNode: by _: for
	// proper blank node syntax

	@Override
	public void runOneIteration() {
		boolean messageReceived = false;
		// process client message
		messageReceived = clientMessageProcessor
				.processMessage(graphHasBeenLoaded);
		graphHasBeenLoaded = clientMessageProcessor
				.isGraphLoaded(graphHasBeenLoaded);
		byte[] receive = getNetworkManager().receive();
		if (receive != null) {
			messageReceived = true;
			// TODO
			logger.info(new String(receive));
		}
		if (!messageReceived) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
		}
	}

	@Override
	public void clearInternal() {
		clientMessageProcessor.clear();
		dictionary.clear();
		statistics.clear();
		// TODO clear slaves
	}

	@Override
	protected void shutDownInternal() {
		clientMessageProcessor.close();
		dictionary.close();
		statistics.close();
	}

	public static void main(String[] args) {
		String className = CidreMaster.class.getName();
		String additionalArgs = "";
		Options options = createCommandLineOptions();
		try {
			CommandLine line = parseCommandLineArgs(options, args);
			Configuration conf = initializeConfiguration(options, line,
					className, additionalArgs);

			CidreMaster master = new CidreMaster(conf);
			master.start();

		} catch (ParseException e) {
			e.printStackTrace();
			printUsage(className, options, additionalArgs);
		}
	}

}
