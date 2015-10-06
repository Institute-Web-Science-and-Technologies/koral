package de.uni_koblenz.west.cidre.master;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.system.CidreSystem;
import de.uni_koblenz.west.cidre.master.client_manager.ClientConnectionManager;
import de.uni_koblenz.west.cidre.master.client_manager.ClientMessageProcessor;

public class CidreMaster extends CidreSystem {

	private final ClientMessageProcessor clientMessageProcessor;

	public CidreMaster(Configuration conf) {
		super(conf, conf.getMaster());
		try {
			ClientConnectionManager clientConnections = new ClientConnectionManager(
					conf, logger);
			clientMessageProcessor = new ClientMessageProcessor(conf,
					clientConnections, logger);
		} catch (Throwable t) {
			if (logger != null) {
				logger.throwing(t.getStackTrace()[0].getClassName(),
						t.getStackTrace()[0].getMethodName(), t);
			}
			throw t;
		}
	}

	@Override
	public void runOneIteration() {
		boolean messageReceived = false;
		// process client message
		messageReceived = clientMessageProcessor.processMessage();
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
	protected void shutDownInternal() {
		clientMessageProcessor.close();
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
