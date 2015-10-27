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
	 * (multipart message)<br>
	 * String ip:port<br>
	 * String command<br>
	 * byte numberOfArgs<br>
	 * byte[] arg1<br>
	 * ...<br>
	 * byte[] arg_{numberOfArgs}
	 */
	CLIENT_COMMAND,

	/**
	 * <p>
	 * master to client<br>
	 * int fileID<br>
	 * long chunkID
	 * </p>
	 * 
	 * <p>
	 * slave to master<br>
	 * short slaveID<br>
	 * int fileID<br>
	 * long chunkID
	 * </p>
	 */
	REQUEST_FILE_CHUNK,

	/**
	 * <p>
	 * client to master<br>
	 * (multipart message)<br>
	 * String ip:port<br>
	 * int fileID<br>
	 * long chunkID<br>
	 * long totalChunks<br>
	 * byte[] file chunk
	 * </p>
	 * 
	 * <p>
	 * master to slave<br>
	 * (multipart message)<br>
	 * int fileID<br>
	 * long chunkID<br>
	 * long totalChunks<br>
	 * byte[] file chunk
	 * </p>
	 */
	FILE_CHUNK,

	/**
	 * master to client<br>
	 * String message
	 */
	MASTER_WORK_IN_PROGRESS,

	/**
	 * client to master<br>
	 * String ip:port<br>
	 * String "|" <br>
	 * String command
	 */
	CLIENT_COMMAND_ABORTED,

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
