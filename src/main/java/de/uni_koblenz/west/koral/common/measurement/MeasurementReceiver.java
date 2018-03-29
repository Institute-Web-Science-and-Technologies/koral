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
package de.uni_koblenz.west.koral.common.measurement;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import de.uni_koblenz.west.koral.common.networManager.NetworkContextFactory;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.zip.GZIPOutputStream;

/**
 * Command line tool that stores the received measurements in a CSV file.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MeasurementReceiver extends Thread implements Closeable {

  private final ZContext context;

  private Socket socket;

  private final Writer writer;

  public MeasurementReceiver(String port, File outputFile) {
    this(null, port, outputFile);
  }

  public MeasurementReceiver(String address, String port, File outputFile) {
    if (!outputFile.getName().endsWith(".gz")) {
      outputFile = new File(outputFile.getAbsolutePath() + ".gz");
    }
    try {
      boolean outputFileExists = outputFile.exists();
      writer = new BufferedWriter(new OutputStreamWriter(
              new GZIPOutputStream(new FileOutputStream(outputFile, true)), "UTF-8"));
      if (outputFileExists) {
        writer.write("\n");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    context = NetworkContextFactory.getNetworkContext();
    socket = context.createSocket(ZMQ.PULL);
    if (address != null) {
      socket.bind("tcp://" + address + ":" + port);
    } else {
      socket.bind("tcp://*:" + port);
    }
  }

  @Override
  public void run() {
    System.out.println(getClass().getName() + " started...");
    Exception mainException = null;
    try {
      writer.write(MeasurementCollector.getHeader());
      while (!isInterrupted()) {
        String recvStr = socket.recvStr(ZMQ.DONTWAIT);
        if (recvStr != null) {
          writer.write(recvStr);
        } else {
          Thread.sleep(100);
        }
      }
    } catch (IOException e) {
      mainException = e;
    } catch (InterruptedException e) {
    } finally {
      if (writer != null) {
        try {
          writer.flush();
          writer.close();
        } catch (IOException e1) {
          if (mainException != null) {
            mainException.addSuppressed(e1);
          } else {
            mainException = e1;
          }
          throw new RuntimeException(mainException);
        }
      }
    }
  }

  @Override
  public void close() {
    if (socket != null) {
      socket.close();
      NetworkContextFactory.destroyNetworkContext(context);
      System.out.println(getClass().getName() + " stopped");
      socket = null;
      if (writer != null) {
        try {
          writer.flush();
          writer.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  public static void main(String[] args) {
    Options options = MeasurementReceiver.createCommandLineOptions();
    try {
      CommandLine line = MeasurementReceiver.parseCommandLineArgs(options, args);
      if (line.hasOption("h")) {
        MeasurementReceiver.printUsage(options);
        return;
      }
      String port = MeasurementCollector.DEFAULT_PORT;
      if (line.hasOption("p")) {
        port = line.getOptionValue("p");
      }

      String address = null;
      if (line.hasOption("i")) {
        address = line.getOptionValue("i");
      }

      String output = null;
      if (line.hasOption("o")) {
        output = line.getOptionValue("o");
      }

      MeasurementReceiver measurementReceiver = new MeasurementReceiver(address, port,
              new File(output));
      measurementReceiver.start();

      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
        @Override
        public void run() {
          measurementReceiver.interrupt();
          measurementReceiver.close();
        }
      }));

    } catch (ParseException e) {
      e.printStackTrace();
      MeasurementReceiver.printUsage(options);
    }
  }

  private static Options createCommandLineOptions() {
    Option help = new Option("h", "help", false, "print this help message");
    help.setRequired(false);

    Option port = Option.builder("p").longOpt("port").hasArg().argName("port")
            .desc("port on which the measurments are received. If no port is specified, port "
                    + MeasurementCollector.DEFAULT_PORT + " is used as default.")
            .required(false).build();

    Option address = Option.builder("i").longOpt("ip").hasArg().argName("ipAddress").desc(
            "specific IP address to which the measurement receiver should be bound. To specifiy the port use the -p option.")
            .required(false).build();

    Option output = Option.builder("o").longOpt("output").hasArg().argName("file.csv")
            .desc("specific CSV file the received measurements are written to.").required(true)
            .build();

    Options options = new Options();
    options.addOption(help);
    options.addOption(port);
    options.addOption(address);
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
    formatter.printHelp("java " + MeasurementReceiver.class.getName()
            + " [-h] [-p <receiverIP:Port>] [-i <bindIPAddress>] -o <file.csv>", options);
  }

}
