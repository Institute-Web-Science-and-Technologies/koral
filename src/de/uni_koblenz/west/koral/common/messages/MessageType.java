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

import de.uni_koblenz.west.koral.common.executor.messagePassing.MessageReceiverListener;
import de.uni_koblenz.west.koral.master.tasks.GraphLoaderListener;
import de.uni_koblenz.west.koral.slave.triple_store.loader.GraphChunkListener;

/**
 * Defines the different types of messages sent between the components of Koral
 * and between the client and Koral master. These constants are used to identify
 * the type of the received binary message. Additionally, it defines the type of
 * {@link MessageListener} that is the receiver of a specific message type.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public enum MessageType {

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
   * int numberOfArgs<br>
   * byte[] arg<sub>1</sub><br>
   * ...<br>
   * byte[] arg<sub>numberOfArgs</sub>
   */
  CLIENT_COMMAND,

  /**
   * master to client<br>
   * String ip:port
   */
  MASTER_SEND_FILES,

  /**
   * client to master<br>
   * String ip:port
   */
  CLIENT_FILES_SENT,

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
   * master to slave (multi-part message)<br>
   * String ipAddress:port<br>
   * String fileName
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
      return GraphLoaderListener.class;
    }
  },

  /**
   * slave to master<br>
   * short slaveID
   */
  GRAPH_LOADING_COMPLETE {
    @Override
    public Class<? extends MessageListener> getListenerType() {
      return GraphLoaderListener.class;
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
   * byte[] (1 byte message type, 8 byte receiving query task id, 8 byte sender
   * task id, 4 byte length of mapping serialization, mapping serialization)+
   */
  QUERY_MAPPING_BATCH {
    @Override
    public Class<? extends MessageListener> getListenerType() {
      return MessageReceiverListener.class;
    }
  },

  /**
   * <p>
   * slave to master<br>
   * short slaveID<br>
   * long id of coordinator task<br>
   * long id of finished query task
   * </p>
   * 
   * <p>
   * slave to slave<br>
   * short slaveID<br>
   * long id of finished query task
   * </p>
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
   * byte[] error message
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
    MessageType[] messageTypes = MessageType.values();
    if (messagePrefix < messageTypes.length) {
      return messageTypes[messagePrefix];
    }
    throw new IllegalArgumentException(
            "There does not exist a message type with prefix " + messagePrefix + ".");
  }

}
