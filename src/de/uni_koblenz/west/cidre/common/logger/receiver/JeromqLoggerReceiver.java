package de.uni_koblenz.west.cidre.common.logger.receiver;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

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

import de.uni_koblenz.west.cidre.common.logger.JeromqStreamHandler;
import de.uni_koblenz.west.cidre.common.networManager.NetworkContextFactory;

public class JeromqLoggerReceiver extends Thread {

	private final ZContext context;

	private final Socket socket;

	private final Writer writer;

	public JeromqLoggerReceiver(String port) {
		context = NetworkContextFactory.getNetworkContext();
		socket = context.createSocket(ZMQ.SUB);
		socket.bind("tcp://*:" + port);
		writer = new OutputStreamWriter(System.out);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void run() {
		Exception mainException = null;
		try {
			while (!isInterrupted()) {
				writer.write(socket.recvStr());
			}
		} catch (IOException e) {
			mainException = e;
		} finally {
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
		socket.close();
		NetworkContextFactory.destroyNetworkContext(context);
	}

	public static void main(String[] args) {
		Options options = createCommandLineOptions();
		try {
			CommandLine line = parseCommandLineArgs(options, args);
			if (line.hasOption("h")) {
				printUsage(options);
				return;
			}
			String port = JeromqStreamHandler.DEFAULT_PORT;
			if (line.hasOption("p")) {
				port = line.getOptionValue("p");
			}

			new JeromqLoggerReceiver(port).start();

			// TODO Auto-generated method stub
		} catch (ParseException e) {
			e.printStackTrace();
			printUsage(options);
		}
	}

	private static Options createCommandLineOptions() {
		Option help = new Option("h", "help", false, "print this help message");
		help.setRequired(false);

		Option port = Option.builder("p").longOpt("port").hasArg()
				.argName("port")
				.desc("port on which the log messages are received. If no port is specified, port "
						+ JeromqStreamHandler.DEFAULT_PORT
						+ " is used as default.")
				.required(false).build();

		Options options = new Options();
		options.addOption(help);
		options.addOption(port);
		return options;
	}

	private static CommandLine parseCommandLineArgs(Options options,
			String[] args) throws ParseException {
		CommandLineParser parser = new DefaultParser();
		return parser.parse(options, args);
	}

	private static void printUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("java " + JeromqLoggerReceiver.class.getName()
				+ " [-h] [-p <receiverIP:Port>] ", options);
	}

}
