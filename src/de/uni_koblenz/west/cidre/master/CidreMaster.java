package de.uni_koblenz.west.cidre.master;

import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import de.uni_koblenz.west.cidre.common.config.Configuration;
import de.uni_koblenz.west.cidre.common.config.XMLDeserializer;
import de.uni_koblenz.west.cidre.common.logger.LoggerFactory;

public class CidreMaster {

	public CidreMaster(Configuration conf) {
		Logger logger = LoggerFactory.getJeromqLogger(conf, conf.getMaster(),
				getClass().getName());
		logger.fine("master started");
		logger.fine("next message");
		// TODO Auto-generated constructor stub
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

			CidreMaster master = new CidreMaster(conf);
			master.toString();
		} catch (ParseException e) {
			e.printStackTrace();
			printUsage(options);
		}
		// TODO Auto-generated method stub

	}

	private static Options createCommandLineOptions() {
		Option help = new Option("h", "help", false, "print this help message");
		help.setRequired(false);

		Option config = Option.builder("c").longOpt("config").hasArg()
				.argName("configFile")
				.desc("the configuration file to use. default is ./cidreConfig.xml")
				.required(false).build();

		Options options = new Options();
		options.addOption(help);
		options.addOption(config);
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
				"java " + CidreMaster.class.getName() + " [-h] -c <configFile>",
				options);
	}

}
