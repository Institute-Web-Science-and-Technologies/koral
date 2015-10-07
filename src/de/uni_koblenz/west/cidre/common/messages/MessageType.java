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
	 * client to master<br>
	 * String ip:port
	 */
	CLIENT_CLOSES_CONNECTION,

	/**
	 * client to master<br>
	 * String ip:port
	 */
	CLIENT_IS_ALIVE,

	/**
	 * client to master<br>
	 * String ip:port<br>
	 * String command \t arg1 \t arg2 \t ...
	 */
	CLIENT_COMMAND,

	/**
	 * master to client<br>
	 * int fileID
	 */
	REQUEST_FILE,

	/**
	 * master to client<br>
	 * int fileID<br>
	 * int chunkID
	 */
	REQUEST_FILE_CHUNK,

	/**
	 * client to master<br>
	 * String ip:port<br>
	 * int fileID<br>
	 * int chunkID<br>
	 * int totalChunks<br>
	 * byte[] file chunk
	 */
	FILE_CHUNK,

	/**
	 * master to client
	 */
	CLIENT_COMMAND_SUCCEEDED,

	/**
	 * master to client<br>
	 * String error message
	 */
	CLIENT_COMMAND_FAILED;

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
