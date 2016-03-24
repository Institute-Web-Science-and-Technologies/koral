package de.uni_koblenz.west.cidre.common.config.utils;

import de.uni_koblenz.west.cidre.common.config.Configurable;
import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.config.impl.XMLDeserializer;

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
      System.out.print(getSerializedValue(conf, args[1]));
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
