package de.uni_koblenz.west.cidre.common.config.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import de.uni_koblenz.west.cidre.common.config.Configurable;
import de.uni_koblenz.west.cidre.common.config.ConfigurableDeserializer;
import de.uni_koblenz.west.cidre.common.config.ConfigurableSerializer;
import de.uni_koblenz.west.cidre.common.config.Property;

public class Configuration implements Configurable {

	private static final String DEFAULT_PORT = "4710";

	@Property(name = "master", description = "The ip and port of the master server, e.g., 192.168.0.1:4710. If no port is specified, the default port "
			+ DEFAULT_PORT + " is used.")
	private String masterIP;

	private String masterPort;

	public String[] getMaster() {
		return new String[] { masterIP, masterPort };
	}

	public void setMaster(String masterIP) {
		setMaster(masterIP, DEFAULT_PORT);
	}

	public void setMaster(String masterIP, String masterPort) {
		this.masterIP = masterIP;
		this.masterPort = masterPort;
	}

	@Property(name = "slaves", description = "The comma separated list of ips and ports of the different slaves, e.g., 192.168.0.2:4712,192.168.0.3,192.168.0.4:4777. If no port is specified, the default port "
			+ DEFAULT_PORT + " is used.")
	private List<String> slaveIPs;

	private List<String> slavePorts;

	public String[] getSlave(int index) {
		return new String[] { slaveIPs.get(index), slavePorts.get(index) };
	}

	public int getNumberOfSlaves() {
		return slaveIPs == null ? 0 : slaveIPs.size();
	}

	public void addSlave(String slaveIP) {
		addSlave(slaveIP, DEFAULT_PORT);
	}

	public void addSlave(String slaveIP, String slavePort) {
		if (slaveIPs == null) {
			slaveIPs = new ArrayList<>();
			slavePorts = new ArrayList<>();
		}
		slaveIPs.add(slaveIP);
		slavePorts.add(slavePort);
	}

	public static final String DEFAULT_CLIENT_PORT = "4711";

	@Property(name = "clientConnection", description = "The ip and port to which clients can connect, e.g., 192.168.0.1:4711. If no port is specified, the default port "
			+ DEFAULT_CLIENT_PORT + " is used.")
	private String clientIP;

	private String clientPort;

	public String[] getClient() {
		return new String[] { clientIP, clientPort };
	}

	public void setClient(String clientIP) {
		setClient(clientIP, DEFAULT_CLIENT_PORT);
	}

	public void setClient(String clientIP, String clientPort) {
		this.clientIP = clientIP;
		this.clientPort = clientPort;
	}

	private String romoteLoggerReceiver;

	public String getRomoteLoggerReceiver() {
		return romoteLoggerReceiver;
	}

	public void setRomoteLoggerReceiver(String romoteLoggerReceiver) {
		this.romoteLoggerReceiver = romoteLoggerReceiver;
	}

	@Property(name = "logLevel", description = "Sets the logging level to one of: OFF, SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST, ALL")
	private Level loglevel = Level.INFO;

	@Property(name = "loggingDirectory", description = "Defines an existing directory in which the log files are written.")
	private String logDirectory = ".";

	public Level getLoglevel() {
		return loglevel;
	}

	public void setLoglevel(Level loglevel) {
		this.loglevel = loglevel;
	}

	public String getLogDirectory() {
		return logDirectory;
	}

	public void setLogDirectory(String logDirectory) {
		this.logDirectory = logDirectory;
	}

	/*
	 * serialization specific code
	 */

	private ConfigurationSerializer serializer;

	private ConfigurationDeserializer deserializer;

	@Override
	public ConfigurableSerializer getSerializer() {
		if (serializer == null) {
			serializer = new ConfigurationSerializer();
		}
		return serializer;
	}

	@Override
	public ConfigurableDeserializer getDeserializer() {
		if (deserializer == null) {
			deserializer = new ConfigurationDeserializer();
		}
		return deserializer;
	}
}
