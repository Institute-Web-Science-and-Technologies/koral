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
package de.uni_koblenz.west.koral.common.executor.messagePassing;

import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.measurement.MeasurementType;
import de.uni_koblenz.west.koral.common.messages.MessageType;
import de.uni_koblenz.west.koral.common.query.Mapping;
import de.uni_koblenz.west.koral.common.query.MappingRecycleCache;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorBase;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

/**
 * <p>
 * This class provides methods to send all required messages during query
 * processing. In the case of sent mappings, it first checks, if the receiver is
 * on the same computer. If so, the message is directly forwarded to the
 * receiver without sending it over the network. Otherwise, it is sent to the
 * remote receiver.
 * </p>
 * 
 * <p>
 * Since there are many small mappings to be sent during the query processing,
 * they are bundled into one large message. These bundles are sent whenever
 * <ul>
 * <li>{@link #close(MappingRecycleCache)} is called,</li>
 * <li>{@link #sendAllBufferedMessages(MappingRecycleCache)} is called,</li>
 * <li>{@link #sendQueryTaskFinished(long, boolean, long, MappingRecycleCache)}
 * is called or</li>
 * <li>the mapping buffer reaches its configured maximum limit.</li>
 * </ul>
 * </p>
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MessageSenderBuffer {

  private final Logger logger;

  private final MeasurementCollector measurementCollector;

  private final long[] sentMessages;

  private final MessageSender messageSender;

  private final MessageReceiverListener localMessageReceiver;

  private final Mapping[][] mappingBuffer;

  private final int[] nextIndex;

  private final int numberOfSlaves;

  public MessageSenderBuffer(int numberOfSlaves, int bundleSize, MessageSender messageSender,
          MessageReceiverListener localMessageReceiver, Logger logger,
          MeasurementCollector measurementCollector) {
    this.logger = logger;
    this.messageSender = messageSender;
    this.localMessageReceiver = localMessageReceiver;
    mappingBuffer = new Mapping[numberOfSlaves + 1][bundleSize];
    nextIndex = new int[numberOfSlaves + 1];
    this.numberOfSlaves = numberOfSlaves;
    this.measurementCollector = measurementCollector;
    sentMessages = new long[numberOfSlaves + 1];
  }

  public int getNumberOfSlaves() {
    return numberOfSlaves;
  }

  public void sendQueryCreate(GraphStatistics statistics, int queryId, QueryOperatorBase queryTree,
          boolean useBaseImplementation) {
    for (int slave = 0; slave < numberOfSlaves; slave++) {
      queryTree.adjustEstimatedLoad(statistics, slave);
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      try (DataOutputStream output2 = new DataOutputStream(output);) {
        output.write(MessageType.QUERY_CREATE.getValue());
        output.write(NumberConversion.int2bytes(queryId));
        queryTree.serialize(output2, useBaseImplementation, slave + 1);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      messageSender.send(slave + 1, output.toByteArray());
    }
  }

  public void sendQueryCreated(int receivingComputer, long coordinatorID) {
    ByteBuffer message = ByteBuffer.allocate(Byte.BYTES + Short.BYTES + Long.BYTES);
    message.put(MessageType.QUERY_CREATED.getValue()).putShort((short) messageSender.getCurrentID())
            .putLong(coordinatorID);
    messageSender.send(receivingComputer, message.array());
  }

  public void sendQueryStart(int queryID) {
    ByteBuffer message = ByteBuffer.allocate(Byte.BYTES + Integer.BYTES);
    message.put(MessageType.QUERY_START.getValue()).putInt(queryID);
    messageSender.sendToAllSlaves(message.array());
  }

  public void sendQueryMapping(Mapping mapping, long senderTaskID, long receiverTaskID,
          MappingRecycleCache mappingCache) {
    mapping.updateReceiver(receiverTaskID);
    mapping.updateSender(senderTaskID);
    int receivingComputer = getComputerID(receiverTaskID);
    if (receivingComputer == messageSender.getCurrentID()) {
      // the receiver is on this computer
      localMessageReceiver.receiveLocalMessage(senderTaskID, receiverTaskID, mapping.getByteArray(),
              mapping.getFirstIndexOfMappingInByteArray(), mapping.getLengthOfMappingInByteArray());
      mappingCache.releaseMapping(mapping);
    } else {
      enqueue(receivingComputer, mapping, receiverTaskID, mappingCache);
    }
  }

  public void sendQueryMappingToAll(Mapping mapping, long senderTaskID, long receiverTaskID,
          MappingRecycleCache mappingCache) {
    receiverTaskID &= 0x00_00_ff_ff_ff_ff_ff_ffl;
    mapping.updateSender(senderTaskID);
    // send to all remote receivers
    for (int i = 1; i < mappingBuffer.length; i++) {
      Mapping newMapping = mappingCache.cloneMapping(mapping);
      long receiver = ((long) i) << (Short.SIZE + Integer.SIZE);
      receiver |= receiverTaskID;
      newMapping.updateReceiver(receiver);
      if (i == messageSender.getCurrentID()) {
        continue;
      } else {
        enqueue(i, newMapping, receiver, mappingCache);
      }
    }
    // send to local receiver (byte array is potentially reused=>receiver
    // entry in array must be correct)
    long receiver = ((long) messageSender.getCurrentID()) << (Short.SIZE + Integer.SIZE);
    receiver |= receiverTaskID;
    mapping.updateReceiver(receiver);
    localMessageReceiver.receiveLocalMessage(senderTaskID, receiver, mapping.getByteArray(),
            mapping.getFirstIndexOfMappingInByteArray(), mapping.getLengthOfMappingInByteArray());
    // release mapping
    mappingCache.releaseMapping(mapping);
  }

  /**
   * Broadcasts the finish message to all instances of this query task on the
   * other computers. If it is the root, the coordinator is informed
   * additionally.
   * 
   * @param finishedTaskID
   * @param isRoot
   * @param coordinatorID
   */
  public void sendQueryTaskFinished(long finishedTaskID, boolean isRoot, long coordinatorID,
          MappingRecycleCache mappingCache) {
    sendAllBufferedMessages(mappingCache);
    ByteBuffer message = ByteBuffer.allocate(Byte.BYTES + Short.BYTES + Long.BYTES);
    message.put(MessageType.QUERY_TASK_FINISHED.getValue())
            .putShort((short) messageSender.getCurrentID()).putLong(finishedTaskID);
    messageSender.sendToAllOtherSlaves(message.array());
    if (isRoot) {
      message = ByteBuffer.allocate(Byte.BYTES + Short.BYTES + Long.BYTES + Long.BYTES);
      message.put(MessageType.QUERY_TASK_FINISHED.getValue())
              .putShort((short) messageSender.getCurrentID()).putLong(coordinatorID)
              .putLong(finishedTaskID);
      messageSender.send(getComputerID(coordinatorID), message.array());
    }
  }

  public void measureSentMessages(int queryID) {
    if (measurementCollector != null) {
      String[] values = new String[sentMessages.length];
      values[0] = Integer.toString(queryID);
      for (int i = 1; i < sentMessages.length; i++) {
        values[i] = Long.toString(sentMessages[i]);
        sentMessages[i] = 0;
      }
      measurementCollector.measureValue(MeasurementType.SLAVE_SENT_MAPPING_BATCHES_TO_SLAVE,
              values);
    }
  }

  private int getComputerID(long taskID) {
    return (int) (taskID >>> (6 * Byte.SIZE));
  }

  public void sendAllBufferedMessages(MappingRecycleCache mappingCache) {
    for (int i = 0; i < mappingBuffer.length; i++) {
      sendBufferedMessages(i, mappingCache);
    }
  }

  private synchronized void sendBufferedMessages(int receivingComputer,
          MappingRecycleCache mappingCache) {
    ByteBuffer buffer = null;
    if (nextIndex[receivingComputer] == 0) {
      // the buffer is empty
      return;
    }
    // determine size of message
    int sizeOfMessage = Byte.BYTES + Short.BYTES;
    for (int i = 0; i < nextIndex[receivingComputer]; i++) {
      Mapping mapping = mappingBuffer[receivingComputer][i];
      sizeOfMessage += mapping.getLengthOfMappingInByteArray();
    }
    // create message
    buffer = ByteBuffer.allocate(sizeOfMessage);
    buffer.put(MessageType.QUERY_MAPPING_BATCH.getValue())
            .putShort((short) messageSender.getCurrentID());
    for (int i = 0; i < nextIndex[receivingComputer]; i++) {
      Mapping mapping = mappingBuffer[receivingComputer][i];
      buffer.put(mapping.getByteArray(), mapping.getFirstIndexOfMappingInByteArray(),
              mapping.getLengthOfMappingInByteArray());
      mappingBuffer[receivingComputer][i] = null;
      mappingCache.releaseMapping(mapping);
    }
    nextIndex[receivingComputer] = 0;
    // send message
    if (buffer != null) {
      messageSender.send(receivingComputer, buffer.array());
      if (measurementCollector != null) {
        sentMessages[receivingComputer] += 1;
      }
    }
  }

  private synchronized void enqueue(int receivingComputer, Mapping mapping, long receiverTaskID,
          MappingRecycleCache mappingCache) {
    if (isBufferFull(receivingComputer)) {
      sendBufferedMessages(receivingComputer, mappingCache);
    }
    mappingBuffer[receivingComputer][nextIndex[receivingComputer]++] = mapping;
    if (isBufferFull(receivingComputer)) {
      sendBufferedMessages(receivingComputer, mappingCache);
    }
  }

  /**
   * Only call it within a synchronized block!
   * 
   * @param receivingComputer
   * @return
   */
  private boolean isBufferFull(int receivingComputer) {
    return nextIndex[receivingComputer] == mappingBuffer[receivingComputer].length;
  }

  public void sendQueryTaskFailed(int receiver, long controllerID, String message) {
    byte[] messageBytes = null;
    try {
      messageBytes = message.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      if (logger != null) {
        logger.finer("Error during conversion of error message during query execution:");
        logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
                e);
      }
      messageBytes = new byte[0];
    }
    ByteBuffer messageBB = ByteBuffer
            .allocate(Byte.BYTES + Short.BYTES + Long.BYTES + messageBytes.length);
    messageBB.put(MessageType.QUERY_TASK_FAILED.getValue())
            .putShort((short) messageSender.getCurrentID()).putLong(controllerID).put(messageBytes);
    messageSender.send(receiver, messageBB.array());
  }

  public void sendQueryAbortion(int queryID) {
    ByteBuffer message = ByteBuffer.allocate(Byte.BYTES + Integer.BYTES);
    message.put(MessageType.QUERY_ABORTION.getValue()).putInt(queryID);
    messageSender.sendToAllSlaves(message.array());
  }

  public void clear() {
    int bufferSize = mappingBuffer[0].length;
    for (int i = 0; i < mappingBuffer.length; i++) {
      synchronized (mappingBuffer[i]) {
        mappingBuffer[i] = new Mapping[bufferSize];
        nextIndex[i] = 0;
      }
    }
    if (measurementCollector != null) {
      for (int i = 0; i < sentMessages.length; i++) {
        sentMessages[i] = 0;
      }
    }
  }

  public void close(MappingRecycleCache mappingCache) {
    sendAllBufferedMessages(mappingCache);
    if (measurementCollector != null) {
      for (int i = 0; i < sentMessages.length; i++) {
        sentMessages[i] = 0;
      }
    }
  }

}
