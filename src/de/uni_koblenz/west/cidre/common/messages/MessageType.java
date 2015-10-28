package de.uni_koblenz.west.cidre.common.messages;

import de.uni_koblenz.west.cidre.common.networManager.MessageListener;
import de.uni_koblenz.west.cidre.master.networkManager.FileChunkRequestListener;

public enum MessageType {

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
	REQUEST_FILE_CHUNK {
		@Override
		public Class<? extends MessageListener> getListenerType() {
			return FileChunkRequestListener.class;
		}
	},

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
	FILE_CHUNK {
		@Override
		public Class<? extends MessageListener> getListenerType() {
			// TODO Auto-generated method stub
			return super.getListenerType();
		}
	},

	/*
	 * client specific messages
	 */

	/**
	 * from master to clients
	 */
	CONNECTION_CLOSED,

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
	CLIENT_COMMAND_FAILED,

	/*
	 * slave specific messages
	 */

	/**
	 * master to slave
	 */
	START_FILE_TRANSFER,

	/**
	 * slave to master<br>
	 * short slaveID
	 */
	FILE_TRANSFER_COMPLETE {
		@Override
		public Class<? extends MessageListener> getListenerType() {
			return FileChunkRequestListener.class;
		}
	};

	public byte getValue() {
		return (byte) ordinal();
	}

	public Class<? extends MessageListener> getListenerType() {
		return null;
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
