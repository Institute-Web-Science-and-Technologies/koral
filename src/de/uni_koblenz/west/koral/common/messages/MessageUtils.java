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
package de.uni_koblenz.west.koral.common.messages;

import java.io.UnsupportedEncodingException;
import java.util.logging.Logger;

/**
 * Provides utitlity methods to create messages.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MessageUtils {

  public static byte[] createStringMessage(MessageType messageType, String message, Logger logger) {
    try {
      byte[] messageBytes = message.getBytes("UTF-8");
      byte[] newMessage = new byte[messageBytes.length + 1];
      System.arraycopy(messageBytes, 0, newMessage, 1, messageBytes.length);
      newMessage[0] = messageType.getValue();
      return newMessage;
    } catch (UnsupportedEncodingException e) {
      if (logger != null) {
        logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
                e);
      }
      throw new RuntimeException(e);
    }
  }

  public static String extractMessageString(byte[] message, Logger logger) {
    byte[] content = new byte[message.length - 1];
    System.arraycopy(message, 1, content, 0, content.length);
    return MessageUtils.convertToString(content, logger);
  }

  public static String convertToString(byte[] message, Logger logger) {
    try {
      return new String(message, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      if (logger != null) {
        logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
                e);
      }
      throw new RuntimeException(e);
    }
  }

}
