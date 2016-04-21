package playground;

import de.uni_koblenz.west.koral.common.config.Configurable;
import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.config.impl.XMLDeserializer;
import de.uni_koblenz.west.koral.common.config.impl.XMLSerializer;

/**
 * If a new configuration option is added to {@link Configuration}, this class
 * updates the existing configuration file and the configuration file tamplate.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class UpdateConfigs {

  public static void main(String[] args) throws InstantiationException, IllegalAccessException {
    UpdateConfigs.update("koralConfig.xml");
    UpdateConfigs.update("koralConfig.xml.template");
  }

  private static void update(String confFile)
          throws InstantiationException, IllegalAccessException {
    Configurable conf = new XMLDeserializer().deserialize(Configuration.class, confFile);
    new XMLSerializer().serialize(conf, confFile);
  }

}
