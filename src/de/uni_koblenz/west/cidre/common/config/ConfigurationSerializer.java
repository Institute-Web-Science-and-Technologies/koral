package de.uni_koblenz.west.cidre.common.config;

class ConfigurationSerializer implements ConfigurableSerializer {

	public String serializeMaster(Configuration conf) {
		String[] master = conf.getMaster();
		if (master[0] != null) {
			return master[0] + ":" + master[1];
		} else {
			return "";
		}
	}

	public String serializeSlaves(Configuration conf) {
		StringBuilder sb = new StringBuilder();
		String delim = "";
		for (int i = 0; i < conf.getNumberOfSlaves(); i++) {
			String[] slave = conf.getSlave(i);
			sb.append(delim).append(slave[0]).append(":").append(slave[1]);
			delim = ",";
		}
		return sb.toString();
	}

}
