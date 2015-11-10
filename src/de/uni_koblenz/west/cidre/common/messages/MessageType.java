package de.uni_koblenz.west.cidre.common.messages;

import de.uni_koblenz.west.cidre.common.executor.messagePassing.MessageReceiverListener;
import de.uni_koblenz.west.cidre.common.networManager.MessageListener;
import de.uni_koblenz.west.cidre.master.networkManager.FileChunkRequestListener;
import de.uni_koblenz.west.cidre.slave.triple_store.loader.GraphChunkListener;

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
	FILE_CHUNK_REQUEST {
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
	FILE_CHUNK_RESPONSE {
		@Override
		public Class<? extends MessageListener> getListenerType() {
			return GraphChunkListener.class;
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
	 * master to client<br>
	 * String result mappings
	 */
	QUERY_RESULT,

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
	 * master to slave<br>
	 * long totalNumberOfChunks
	 */
	START_FILE_TRANSFER {
		@Override
		public Class<? extends MessageListener> getListenerType() {
			return GraphChunkListener.class;
		}
	},

	/**
	 * slave to master<br>
	 * short slaveID<br>
	 * String error message
	 */
	GRAPH_LOADING_FAILED {
		@Override
		public Class<? extends MessageListener> getListenerType() {
			return FileChunkRequestListener.class;
		}
	},

	/**
	 * slave to master<br>
	 * short slaveID
	 */
	GRAPH_LOADING_COMPLETE {
		@Override
		public Class<? extends MessageListener> getListenerType() {
			return FileChunkRequestListener.class;
		}
	},

	/**
	 * master to all slaves<br>
	 * int query id<br>
	 * byte[] query tree serialization
	 */
	QUERY_CREATE,

	/**
	 * slave to master<br>
	 * short slaveID<br>
	 * long id of receiving query node
	 */
	QUERY_CREATED,

	/**
	 * master to all slaves<br>
	 * int query id
	 */
	QUERY_START,

	/**
	 * master to all slaves<br>
	 * int query id
	 */
	QUERY_ABORTION,

	/**
	 * slave to slave, slave to master<br>
	 * short slaveID<br>
	 * long receiving query task id<br>
	 * byte[] mapping serialization
	 */
	QUERY_MAPPING_BATCH {
		@Override
		public Class<? extends MessageListener> getListenerType() {
			return MessageReceiverListener.class;
		}
	},

	/**
	 * slave to slave, slave to master<br>
	 * short slaveID<br>
	 * long id of finished query task
	 */
	QUERY_TASK_FINISHED {
		@Override
		public Class<? extends MessageListener> getListenerType() {
			return MessageReceiverListener.class;
		}
	},

	/**
	 * slave to master<br>
	 * short slaveID<br>
	 * long receiving query task id<br>
	 * String error message
	 */
	QUERY_TASK_FAILED {
		@Override
		public Class<? extends MessageListener> getListenerType() {
			return MessageReceiverListener.class;
		}
	},

	/**
	 * master to slave
	 */
	CLEAR;

	public byte getValue() {
		return (byte) ordinal();
	}

	public Class<? extends MessageListener> getListenerType() {
		return null;
	}

	public static MessageType valueOf(byte messagePrefix) {
		MessageType[] messageTypes = values();
		if (messagePrefix < messageTypes.length) {
			return messageTypes[messagePrefix];
		}
		throw new IllegalArgumentException(
				"There does not exist a message type with prefix "
						+ messagePrefix + ".");
	}

}
