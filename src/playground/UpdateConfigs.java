package playground;

import de.uni_koblenz.west.cidre.common.config.Configurable;
import de.uni_koblenz.west.cidre.common.config.Configuration;
import de.uni_koblenz.west.cidre.common.config.XMLDeserializer;
import de.uni_koblenz.west.cidre.common.config.XMLSerializer;

public class UpdateConfigs {

	public static void main(String[] args)
			throws InstantiationException, IllegalAccessException {
		update("cidreConfig.xml");
		update("cidreConfig.xml.template");
	}

	private static void update(String confFile)
			throws InstantiationException, IllegalAccessException {
		Configurable conf = new XMLDeserializer()
				.deserialize(Configuration.class, confFile);
		new XMLSerializer().serialize(conf, confFile);
	}

}
