package de.uni_koblenz.west.cidre.common.config;

public interface Configurable {

	public ConfigurableSerializer getSerializer();

	public ConfigurableDeserializer getDeserializer();

}
