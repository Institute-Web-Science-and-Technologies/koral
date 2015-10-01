package de.uni_koblenz.west.cidre.common.messages;

public enum MessageType {

	CLIENT_CONNECTION_CREATION, CLIENT_CONNECTION_CONFIRMATION, CLIENT_CLOSES_CONNECTION, CLIENT_IS_ALIVE;

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
