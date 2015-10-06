package de.uni_koblenz.west.cidre.common.messages;

public enum MessageType {

	/**
	 * from master to clients
	 */
	CONNECTION_CLOSED,

	/*
	 * client specific messages
	 */

	/**
	 * client to master<br>
	 * String ip:port
	 */
	CLIENT_CONNECTION_CREATION,

	/**
	 * master to client
	 */
	CLIENT_CONNECTION_CONFIRMATION,

	/**
	 * client to master
	 */
	CLIENT_CLOSES_CONNECTION,

	/**
	 * client to master
	 */
	CLIENT_IS_ALIVE;

	public byte getValue() {
		return (byte) ordinal();
	}

	public static MessageType valueOf(byte messagePrefix) {
		MessageType[] messageTypes = values();
		if (messagePrefix < messageTypes.length && messagePrefix != 0) {
			return messageTypes[messagePrefix];
		}
		throw new IllegalArgumentException(
				"There does not exist a message type with prefix "
						+ messagePrefix + ".");
	}

}
