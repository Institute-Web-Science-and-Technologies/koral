package de.uni_koblenz.west.cidre;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.config.impl.XMLSerializer;

/**
 * A command line tool to create a configuration file with the default settings.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class DefaultConfigurationCreator {

	public static void main(String[] args) {
		Options options = createCommandLineOptions();
		try {
			CommandLine line = parseCommandLineArgs(options, args);
			if (line.hasOption("h")) {
				printUsage(options);
				return;
			}
			String outputFile = "cidreConfig.xml";
			if (line.hasOption("o")) {
				outputFile = line.getOptionValue("o");
			}
			new XMLSerializer().serialize(new Configuration(), outputFile);
		} catch (ParseException e) {
			e.printStackTrace();
			printUsage(options);
		}
	}

	private static Options createCommandLineOptions() {
		Option help = new Option("h", "help", false, "print this help message");
		help.setRequired(false);

		Option output = Option.builder("o").longOpt("output").hasArg()
				.argName("configFile")
				.desc("the file where the default configuration should be created. default is ./cidreConfig.xml")
				.required(false).build();

		Options options = new Options();
		options.addOption(help);
		options.addOption(output);
		return options;
	}

	private static CommandLine parseCommandLineArgs(Options options,
			String[] args) throws ParseException {
		CommandLineParser parser = new DefaultParser();
		return parser.parse(options, args);
	}

	private static void printUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter
				.printHelp("java " + DefaultConfigurationCreator.class.getName()
						+ " [-h] -o <configFile>", options);
	}

}
