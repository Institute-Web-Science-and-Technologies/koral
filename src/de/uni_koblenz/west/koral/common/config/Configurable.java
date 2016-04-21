package de.uni_koblenz.west.koral.common.config;

/**
 * Interface for all configurations that should be (de)serializable in a
 * configuration file.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface Configurable {

  public ConfigurableSerializer getSerializer();

  public ConfigurableDeserializer getDeserializer();

}
