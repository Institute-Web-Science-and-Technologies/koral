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
package de.uni_koblenz.west.koral.common.logger.receiver;

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

import de.uni_koblenz.west.koral.common.logger.JeromqStreamHandler;
import de.uni_koblenz.west.koral.common.networManager.NetworkContextFactory;

import java.io.IOException;
import java.io.Writer;

/**
 * Command line tool that prints the logging messages received from each Koral
 * master and slave to the console.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class JeromqLoggerReceiver extends Thread {

  private final ZContext context;

  private Socket socket;

  private final Writer writer;

  public JeromqLoggerReceiver(String port) {
    this(null, port);
  }

  public JeromqLoggerReceiver(String address, String port) {
    context = NetworkContextFactory.getNetworkContext();
    socket = context.createSocket(ZMQ.PULL);
    if (address != null) {
      socket.bind("tcp://" + address + ":" + port);
    } else {
      socket.bind("tcp://*:" + port);
    }
    writer = null;
  }

  @Override
  public void run() {
    System.out.println(getClass().getName() + " started...");
    Exception mainException = null;
    try {
      while (!isInterrupted()) {
        String recvStr = socket.recvStr(ZMQ.DONTWAIT);
        if (recvStr != null) {
          if (writer == null) {
            System.out.println(recvStr);
          } else {
            writer.write(recvStr);
          }
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

  public void shutDown() {
    if (socket != null) {
      socket.close();
      NetworkContextFactory.destroyNetworkContext(context);
      System.out.println(getClass().getName() + " stopped");
      socket = null;
    }
  }

  public static void main(String[] args) {
    Options options = JeromqLoggerReceiver.createCommandLineOptions();
    try {
      CommandLine line = JeromqLoggerReceiver.parseCommandLineArgs(options, args);
      if (line.hasOption("h")) {
        JeromqLoggerReceiver.printUsage(options);
        return;
      }
      String port = JeromqStreamHandler.DEFAULT_PORT;
      if (line.hasOption("p")) {
        port = line.getOptionValue("p");
      }

      String address = null;
      if (line.hasOption("i")) {
        address = line.getOptionValue("i");
      }

      JeromqLoggerReceiver jeromqLoggerReceiver = new JeromqLoggerReceiver(address, port);
      jeromqLoggerReceiver.start();

      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
        @Override
        public void run() {
          jeromqLoggerReceiver.interrupt();
          jeromqLoggerReceiver.shutDown();
        }
      }));

    } catch (ParseException e) {
      e.printStackTrace();
      JeromqLoggerReceiver.printUsage(options);
    }
  }

  private static Options createCommandLineOptions() {
    Option help = new Option("h", "help", false, "print this help message");
    help.setRequired(false);

    Option port = Option.builder("p").longOpt("port").hasArg().argName("port")
            .desc("port on which the log messages are received. If no port is specified, port "
                    + JeromqStreamHandler.DEFAULT_PORT + " is used as default.")
            .required(false).build();

    Option address = Option.builder("i").longOpt("ip").hasArg().argName("ipAddress")
            .desc("specific IP address to which the log receiver should be bound. To specifiy the port use the -p option.")
            .required(false).build();

    Options options = new Options();
    options.addOption(help);
    options.addOption(port);
    options.addOption(address);
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
            "java " + JeromqLoggerReceiver.class.getName() + " [-h] [-p <receiverIP:Port>] ",
            options);
  }

}
