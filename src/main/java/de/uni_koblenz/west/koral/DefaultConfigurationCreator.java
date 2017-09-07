/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License
 * along with Koral.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
package de.uni_koblenz.west.koral;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.config.impl.XMLSerializer;

/**
 * A command line tool to create a configuration file with the default settings.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class DefaultConfigurationCreator {

  public static void main(String[] args) {
    Options options = DefaultConfigurationCreator.createCommandLineOptions();
    try {
      CommandLine line = DefaultConfigurationCreator.parseCommandLineArgs(options, args);
      if (line.hasOption("h")) {
        DefaultConfigurationCreator.printUsage(options);
        return;
      }
      String outputFile = "koralConfig.xml";
      if (line.hasOption("o")) {
        outputFile = line.getOptionValue("o");
      }
      new XMLSerializer().serialize(new Configuration(), outputFile);
    } catch (ParseException e) {
      e.printStackTrace();
      DefaultConfigurationCreator.printUsage(options);
    }
  }

  private static Options createCommandLineOptions() {
    Option help = new Option("h", "help", false, "print this help message");
    help.setRequired(false);

    Option output = Option.builder("o").longOpt("output").hasArg().argName("configFile")
            .desc("the file where the default configuration should be created. default is ./koralConfig.xml")
            .required(false).build();

    Options options = new Options();
    options.addOption(help);
    options.addOption(output);
    return options;
  }

  private static CommandLine parseCommandLineArgs(Options options, String[] args)
          throws ParseException {
    CommandLineParser parser = new DefaultParser();
    return parser.parse(options, args);
  }

  private static void printUsage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(
            "java " + DefaultConfigurationCreator.class.getName() + " [-h] -o <configFile>",
            options);
  }

}
