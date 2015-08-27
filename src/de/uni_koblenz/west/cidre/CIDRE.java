package de.uni_koblenz.west.cidre;

import de.uni_koblenz.west.cidre.master.CidreMaster;
import de.uni_koblenz.west.cidre.slave.CidreSlave;

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
		}
		printUsage();
	}

	private static void printUsage() {
		System.out.println(
				"java " + CIDRE.class.getName() + " master <argsOfMaster>");
		System.out.println(
				"java " + CIDRE.class.getName() + " slave <argsOfSlave>");
	}

}
