package de.uni_koblenz.west.cidre.slave;

import java.net.Inet4Address;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.system.CidreSystem;
import de.uni_koblenz.west.cidre.common.system.ConfigurationException;

public class CidreSlave extends CidreSystem {

	public CidreSlave(Configuration conf) throws ConfigurationException {
		super(conf, getCurrentIP(conf));
	}

	private static String[] getCurrentIP(Configuration conf)
			throws ConfigurationException {
		for (int i = 0; i < conf.getNumberOfSlaves(); i++) {
			String[] slave = conf.getSlave(i);
			try {
				NetworkInterface ni = NetworkInterface
						.getByInetAddress(Inet4Address.getByName(slave[0]));
				if (ni != null) {
					return slave;
				}
			} catch (SocketException | UnknownHostException e) {
			}
		}
		throw new ConfigurationException(
				"The current slave cannot be found in the configuration file.");
	}

	@Override
	public void runOneIteration() {
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// TODO
		interrupt();
	}

	@Override
	protected void shutDownInternal() {
	}

	public static void main(String[] args) {
		String className = CidreSlave.class.getName();
		String additionalArgs = "";
		Options options = createCommandLineOptions();
		try {
			CommandLine line = parseCommandLineArgs(options, args);
			Configuration conf = initializeConfiguration(options, line,
					className, additionalArgs);

			CidreSlave slave;
			try {
				slave = new CidreSlave(conf);
				slave.start();
			} catch (ConfigurationException e) {
				e.printStackTrace();
			}

		} catch (ParseException e) {
			e.printStackTrace();
			printUsage(className, options, additionalArgs);
		}
	}

}
