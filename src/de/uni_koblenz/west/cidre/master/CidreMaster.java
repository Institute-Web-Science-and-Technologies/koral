package de.uni_koblenz.west.cidre.master;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.system.CidreSystem;

public class CidreMaster extends CidreSystem {

	public CidreMaster(Configuration conf) {
		super(conf, conf.getMaster());
	}

	@Override
	public void runOneIteration() {
		try {
			Thread.sleep(1000);
			logger.info(new String(getNetworkManager().receive()));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// TODO
		// interrupt();
	}

	@Override
	protected void shutDownInternal() {
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
