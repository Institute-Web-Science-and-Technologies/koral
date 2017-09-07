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
import de.uni_koblenz.west.koral.common.config.Property;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

/**
 * Implements a final state machine to load the configuration file into a
 * {@link Configuration} instance.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
enum ConfigurableDeserializerState {
  STARTED {
    @Override
    public ConfigurableDeserializerState performTransition(Configurable conf, XMLEvent event) {
      if (event.isStartElement()) {
        StartElement startElement = event.asStartElement();
        if (startElement.getName().getLocalPart().equals(XMLConstants.CONFIG_ELEMENT)) {
          return ROOT_ELEMENT;
        }
      }
      return this;
    }
  },
  ROOT_ELEMENT {
    @Override
    public ConfigurableDeserializerState performTransition(Configurable conf, XMLEvent event) {
      if (event.isStartElement()) {
        StartElement startElement = event.asStartElement();
        if (startElement.getName().getLocalPart().equals(XMLConstants.PROPERTY_ELEMENT)) {
          return PROPERTY;
        }
      } else if (event.isEndElement()) {
        EndElement endElement = event.asEndElement();
        if (endElement.getName().getLocalPart().equals(XMLConstants.CONFIG_ELEMENT)) {
          return FINISHED;
        }
      }
      return this;
    }
  },
  PROPERTY {
    @Override
    public ConfigurableDeserializerState performTransition(Configurable conf, XMLEvent event) {
      if (event.isStartElement()) {
        StartElement startElement = event.asStartElement();
        String localName = startElement.getName().getLocalPart();
        if (localName.equals(XMLConstants.PROPERTY_NAME)) {
          return PROPERTY_NAME;
        } else if (localName.equals(XMLConstants.PROPERTY_VALUE)) {
          return PROPERTY_VALUE;
        }
      } else if (event.isEndElement()) {
        EndElement endElement = event.asEndElement();
        if (endElement.getName().getLocalPart().equals(XMLConstants.PROPERTY_ELEMENT)) {
          return ROOT_ELEMENT;
        }
      }
      return this;
    }
  },
  PROPERTY_NAME {

    @Override
    public ConfigurableDeserializerState performTransition(Configurable conf, XMLEvent event) {
      if (event.isCharacters()) {
        PROPERTY_VALUE.setPropertyName(event.asCharacters().getData());
      } else if (event.isEndElement()) {
        EndElement endElement = event.asEndElement();
        if (endElement.getName().getLocalPart().equals(XMLConstants.PROPERTY_NAME)) {
          return PROPERTY;
        }
      }
      return this;
    }
  },
  PROPERTY_VALUE {

    private String propertyName;

    @Override
    public ConfigurableDeserializerState performTransition(Configurable conf, XMLEvent event)
            throws NoSuchMethodException, SecurityException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException {
      if (event.isCharacters()) {
        setProperty(conf, event.asCharacters().getData());
      } else if (event.isEndElement()) {
        EndElement endElement = event.asEndElement();
        if (endElement.getName().getLocalPart().equals(XMLConstants.PROPERTY_VALUE)) {
          return PROPERTY;
        }
      }
      return this;
    }

    @Override
    protected void setPropertyName(String propertyName) {
      this.propertyName = propertyName;
    }

    private void setProperty(Configurable conf, String data)
            throws NoSuchMethodException, SecurityException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException {
      for (Field field : conf.getClass().getDeclaredFields()) {
        Property annotation = field.getAnnotation(Property.class);
        if ((annotation != null) && annotation.name().equals(propertyName)) {
          setSerializedValue(conf, annotation.name(), data);
        }
      }
    }

    private void setSerializedValue(Configurable conf, String fieldName, String data)
            throws NoSuchMethodException, SecurityException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException {
      String methodName = "deserialize" + Character.toUpperCase(fieldName.charAt(0))
              + fieldName.substring(1);
      Method serializeMethod = conf.getDeserializer().getClass().getMethod(methodName,
              conf.getClass(), String.class);
      serializeMethod.invoke(conf.getDeserializer(), conf, data);
    }
  },
  FINISHED {
    @Override
    public ConfigurableDeserializerState performTransition(Configurable conf, XMLEvent event) {
      return this;
    }
  };

  public abstract ConfigurableDeserializerState performTransition(Configurable conf, XMLEvent event)
          throws NoSuchMethodException, SecurityException, IllegalAccessException,
          IllegalArgumentException, InvocationTargetException;

  protected void setPropertyName(String propertyName) {

  }
}
