package de.uni_koblenz.west.cidre;

import de.uni_koblenz.west.cidre.client.CidreClient;
import de.uni_koblenz.west.cidre.common.logger.receiver.JeromqLoggerReceiver;
import de.uni_koblenz.west.cidre.master.CidreMaster;
import de.uni_koblenz.west.cidre.slave.CidreSlave;

/**
 * Provides a unique command line user interface to start any component of
 * CIDRE.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class CIDRE {

	public static void main(String[] args) {
		if (args.length < 1) {
			printUsage();
		}
		String[] followUpArgs = new String[args.length - 1];
		System.arraycopy(args, 1, followUpArgs, 0, followUpArgs.length);
		switch (args[0].toLowerCase()) {
		case "master":
			CidreMaster.main(followUpArgs);
			return;
		case "slave":
			CidreSlave.main(followUpArgs);
			return;
		case "client":
			CidreClient.main(followUpArgs);
			return;
		case "logreceiver":
			JeromqLoggerReceiver.main(followUpArgs);
			return;
		}
		printUsage();
	}

	private static void printUsage() {
		System.out.println(
				"java " + CIDRE.class.getName() + " master <argsOfMaster>");
		System.out.println(
				"java " + CIDRE.class.getName() + " slave <argsOfSlave>");
		System.out.println(
				"java " + CIDRE.class.getName() + " client <argsOfClient>");
		System.out.println("java " + CIDRE.class.getName()
				+ " logReceiver <argsOfLogReceiver>");
	}

}
