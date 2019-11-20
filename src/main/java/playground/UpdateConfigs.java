/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License
 * along with Koral.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
package playground;

import de.uni_koblenz.west.koral.common.config.Configurable;
import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.config.impl.XMLDeserializer;
import de.uni_koblenz.west.koral.common.config.impl.XMLSerializer;

import java.lang.reflect.InvocationTargetException;

/**
 * If a new configuration option is added to {@link Configuration}, this class
 * updates the existing configuration file and the configuration file tamplate.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class UpdateConfigs {

  public static void main(String[] args)
          throws InstantiationException, IllegalAccessException, IllegalArgumentException,
          InvocationTargetException, NoSuchMethodException, SecurityException {
    UpdateConfigs.update("koralConfig.xml");
    UpdateConfigs.update("koralConfig.xml.template");
  }

  private static void update(String confFile)
          throws InstantiationException, IllegalAccessException, IllegalArgumentException,
          InvocationTargetException, NoSuchMethodException, SecurityException {
    Configurable conf = new XMLDeserializer().deserialize(Configuration.class, confFile);
    new XMLSerializer().serialize(conf, confFile);
  }

}
