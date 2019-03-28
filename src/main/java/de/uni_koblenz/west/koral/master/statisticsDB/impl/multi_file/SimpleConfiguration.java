package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import java.util.HashMap;

/**
 * Singleton class that holds configuration values set via CLI in StatisticsDBTest main class. Primarily developed to
 * make setting a value deep in the class structure easier, without having to pass it through 10+ constructors.
 *
 * @author Philipp Töws
 *
 */
public class SimpleConfiguration {

	private static SimpleConfiguration instance;

	private final HashMap<ConfigurationKey, Object> config;

	public static enum ConfigurationKey {
		RLE_EXTENSION_LENGTH,
		SLRU_PROTECTED_MIN_HITS
	}

	private SimpleConfiguration() {
		config = new HashMap<>();
	}

	public static SimpleConfiguration getInstance() {
		if (instance == null) {
			instance = new SimpleConfiguration();
		}
		return instance;
	}

	public void setValue(ConfigurationKey key, Object value) {
		config.put(key, value);
	}

	public Object getValue(ConfigurationKey key) {
		return config.get(key);
	}

}
