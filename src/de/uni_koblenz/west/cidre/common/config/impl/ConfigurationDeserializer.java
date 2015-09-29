package de.uni_koblenz.west.cidre.common.config.impl;

import java.util.regex.Pattern;

import de.uni_koblenz.west.cidre.common.config.ConfigurableDeserializer;

class ConfigurationDeserializer implements ConfigurableDeserializer {

	public void deserializeMaster(Configuration conf, String master) {
		if (master.indexOf(':') == -1) {
			conf.setMaster(master);
		} else {
			String[] parts = master.split(Pattern.quote(":"));
			conf.setMaster(parts[0], parts[1]);
		}
	}

	public void deserializeSlaves(Configuration conf, String slaves) {
		String[] entries = slaves.split(Pattern.quote(","));
		for (int i = 0; i < entries.length; i++) {
			String entry = entries[0];
			if (entry.indexOf(':') == -1) {
				conf.addSlave(entry);
			} else {
				String[] parts = entry.split(Pattern.quote(":"));
				conf.addSlave(parts[0], parts[1]);
			}
		}
	}

}
