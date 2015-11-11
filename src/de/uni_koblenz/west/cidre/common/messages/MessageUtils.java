package de.uni_koblenz.west.cidre.common.messages;

import java.io.UnsupportedEncodingException;
import java.util.logging.Logger;

/**
 * Provides utitlity methods to create messages.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MessageUtils {

	public static byte[] createStringMessage(MessageType messageType,
			String message, Logger logger) {
		try {
			byte[] messageBytes = message.getBytes("UTF-8");
			byte[] newMessage = new byte[messageBytes.length + 1];
			System.arraycopy(messageBytes, 0, newMessage, 1,
					messageBytes.length);
			newMessage[0] = messageType.getValue();
			return newMessage;
		} catch (UnsupportedEncodingException e) {
			if (logger != null) {
				logger.throwing(e.getStackTrace()[0].getClassName(),
						e.getStackTrace()[0].getMethodName(), e);
			}
			throw new RuntimeException(e);
		}
	}

	public static String extreactMessageString(byte[] message, Logger logger) {
		byte[] content = new byte[message.length - 1];
		System.arraycopy(message, 1, content, 0, content.length);
		return convertToString(content, logger);
	}

	public static String convertToString(byte[] message, Logger logger) {
		try {
			return new String(message, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			if (logger != null) {
				logger.throwing(e.getStackTrace()[0].getClassName(),
						e.getStackTrace()[0].getMethodName(), e);
			}
			throw new RuntimeException(e);
		}
	}

}
