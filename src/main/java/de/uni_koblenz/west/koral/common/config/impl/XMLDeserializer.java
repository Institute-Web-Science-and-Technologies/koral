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
package de.uni_koblenz.west.koral.common.config.impl;

import de.uni_koblenz.west.koral.common.config.Configurable;
import de.uni_koblenz.west.koral.common.config.DeserializationException;

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
          String confFileName)
          throws InstantiationException, IllegalAccessException, IllegalArgumentException,
          InvocationTargetException, NoSuchMethodException, SecurityException {
    return deserialize(configurableClass, new File(confFileName));
  }

  public Configurable deserialize(Class<? extends Configurable> configurableClass, File confFile)
          throws InstantiationException, IllegalAccessException, IllegalArgumentException,
          InvocationTargetException, NoSuchMethodException, SecurityException {
    try (BufferedReader br = new BufferedReader(
            new InputStreamReader(new FileInputStream(confFile), "UTF-8"));) {
      return deserialize(configurableClass, br);
    } catch (IOException e) {
      e.printStackTrace();
      throw new DeserializationException(e);
    }
  }

  public Configurable deserialize(Class<? extends Configurable> configurableClass,
          Reader confFileReader)
          throws InstantiationException, IllegalAccessException, IllegalArgumentException,
          InvocationTargetException, NoSuchMethodException, SecurityException {
    Configurable conf = configurableClass.getDeclaredConstructor().newInstance();
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
