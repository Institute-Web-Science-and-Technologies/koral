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
package de.uni_koblenz.west.koral.common.config.utils;

import de.uni_koblenz.west.koral.common.config.Configurable;
import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.config.impl.XMLDeserializer;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Command line tool that allows reading one property from the configuration
 * file. Useful if paths are required in bash scripts.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class ConfigCLI {

  public static void main(String[] args) {
    if (args.length != 2) {
      System.out.println(
              "Usage: " + ConfigCLI.class.getName() + " <pathToConfig.xml> <nameOfProperty>");
      return;
    }
    Configuration conf = new Configuration();
    new XMLDeserializer().deserialize(conf, new File(args[0]));

    try {
      System.out.print(ConfigCLI.getSerializedValue(conf, args[1]));
    } catch (NoSuchMethodException | SecurityException | IllegalAccessException
            | IllegalArgumentException | InvocationTargetException e) {
      e.printStackTrace();
    }
  }

  private static String getSerializedValue(Configurable conf, String fieldName)
          throws NoSuchMethodException, SecurityException, IllegalAccessException,
          IllegalArgumentException, InvocationTargetException {
    String methodName = "serialize" + Character.toUpperCase(fieldName.charAt(0))
            + fieldName.substring(1);
    Method serializeMethod = conf.getSerializer().getClass().getMethod(methodName, conf.getClass());
    return serializeMethod.invoke(conf.getSerializer(), conf).toString();
  }

}
