package de.uni_koblenz.west.cidre.common.config.impl;

import de.uni_koblenz.west.cidre.common.config.Configurable;
import de.uni_koblenz.west.cidre.common.config.DeserializationException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

/**
 * Loads an XML configuration file into a {@link Configurable} instance.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class XMLDeserializer {

  public Configurable deserialize(Class<? extends Configurable> configurableClass,
          String confFileName) throws InstantiationException, IllegalAccessException {
    return deserialize(configurableClass, new File(confFileName));
  }

  public Configurable deserialize(Class<? extends Configurable> configurableClass, File confFile)
          throws InstantiationException, IllegalAccessException {
    try (BufferedReader br = new BufferedReader(
            new InputStreamReader(new FileInputStream(confFile), "UTF-8"));) {
      return deserialize(configurableClass, br);
    } catch (IOException e) {
      e.printStackTrace();
      throw new DeserializationException(e);
    }
  }

  public Configurable deserialize(Class<? extends Configurable> configurableClass,
          Reader confFileReader) throws InstantiationException, IllegalAccessException {
    Configurable conf = configurableClass.newInstance();
    deserialize(conf, confFileReader);
    return conf;
  }

  public void deserialize(Configurable conf, String confFileName) {
    deserialize(conf, new File(confFileName));
  }

  public void deserialize(Configurable conf, File confFile) {
    try (BufferedReader br = new BufferedReader(
            new InputStreamReader(new FileInputStream(confFile), "UTF-8"));) {
      deserialize(conf, br);
    } catch (IOException e) {
      e.printStackTrace();
      throw new DeserializationException(e);
    }
  }

  public void deserialize(Configurable conf, Reader confFileReader) {
    XMLEventReader reader = null;
    try {
      reader = XMLInputFactory.newInstance().createXMLEventReader(confFileReader);
      deserializeDocument(conf, reader);
      reader.close();
    } catch (XMLStreamException | FactoryConfigurationError | NoSuchMethodException
            | SecurityException | IllegalAccessException | IllegalArgumentException
            | InvocationTargetException e) {
      e.printStackTrace();
      if (reader != null) {
        try {
          reader.close();
        } catch (XMLStreamException e1) {
          e1.printStackTrace();
          e.addSuppressed(e1);
        }
      }
      throw new DeserializationException(e);
    }
  }

  private void deserializeDocument(Configurable conf, XMLEventReader reader)
          throws XMLStreamException, NoSuchMethodException, SecurityException,
          IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    ConfigurableDeserializerState currentState = ConfigurableDeserializerState.STARTED;
    while (reader.hasNext()) {
      XMLEvent event = reader.nextEvent();
      currentState = currentState.performTransition(conf, event);
    }
  }

}
