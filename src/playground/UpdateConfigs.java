package playground;

import de.uni_koblenz.west.cidre.common.config.Configurable;
import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.config.impl.XMLDeserializer;
import de.uni_koblenz.west.cidre.common.config.impl.XMLSerializer;

/**
 * If a new configuration option is added to {@link Configuration}, this class
 * updates the existing configuration file and the configuration file tamplate.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class UpdateConfigs {

  public static void main(String[] args) throws InstantiationException, IllegalAccessException {
    update("cidreConfig.xml");
    update("cidreConfig.xml.template");
  }

  private static void update(String confFile)
          throws InstantiationException, IllegalAccessException {
    Configurable conf = new XMLDeserializer().deserialize(Configuration.class, confFile);
    new XMLSerializer().serialize(conf, confFile);
  }

}
