package de.uni_koblenz.west.cidre.common.config;

import java.util.ArrayList;
import java.util.List;

public class Configuration implements Configurable {

	private static final String DEFAULT_PORT = "4711";

	@Property(name = "master", description = "The ip and port of the master server, e.g., 192.168.0.1:4711. If no port is specified, the default port "
			+ DEFAULT_PORT + " is used.")
	private String masterIP;

	private String masterPort;

	@Property(name = "slaves", description = "The comma separated list of ips and ports of the different slaves, e.g., 192.168.0.2:4712,192.168.0.3,192.168.0.4:4777. If no port is specified, the default port "
			+ DEFAULT_PORT + " is used.")
	private List<String> slaveIPs;

	private List<String> slavePorts;

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

	/*
	 * serializer specific code
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
