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

import de.uni_koblenz.west.koral.common.executor.WorkerTask;
import de.uni_koblenz.west.koral.common.messages.MessageListener;
import de.uni_koblenz.west.koral.common.messages.MessageType;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;

import java.nio.BufferUnderflowException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * <p>
 * All messages related to query processing received by Koral master or a slave
 * are propagated to this class. According to the receiver task id of the
 * message, the message is put in the input message queue of the corresponding
 * {@link WorkerTask}.
 * </p>
 * 
 * <p>
 * This class maintains pointers to all {@link WorkerTask} that are currently
 * registered. Since a huge amount of messages in a high frequency is expected,
 * the access to the {@link WorkerTask}s should be very fast. The usage of a
 * {@link Map} is not possible, since the receiver task ids would have to be
 * packed into an array or a {@link Long} instances that would be thrown away
 * after the lookup.
 * </p>
 * 
 * <p>
 * The receiver task id has the following structure:
 * <ul>
 * <li>2 bytes identifying the computer on which the task is executed</li>
 * <li>4 bytes identifying the query to which the task belongs</li>
 * <li>2 bytes identifying the task of this query</li>
 * </ul>
 * </p>
 * 
 * <p>
 * With the knowledge about the receiver task id structure, the
 * {@link WorkerTask}s can be efficiently accessed by an six dimensional array.
 * Each dimension stands for one of the third till eighth byte of the id. In
 * order to access the correct index the bytes have to be interpreted unsigned.
 * </p>
 * 
 * <p>
 * To avoid an excessive waste of memory, this implementation requires that the
 * query and task ids start with 0 and are reused as soon as the query is
 * finished.
 * </p>
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MessageReceiverListener implements MessageListener {

  private final Logger logger;

  private WorkerTask[][][][][][] taskRegistry;

  public MessageReceiverListener(Logger logger) {
    this.logger = logger;
  }

  @Override
  public int getSlaveID() {
    return Integer.MAX_VALUE;
  }

  public void register(WorkerTask task) {
    byte[] id = NumberConversion.long2bytes(task.getID());
    registerTask(id, task);
  }

  @Override
  public void processMessage(byte[][] message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void processMessage(byte[] message) {
    MessageType messageType = null;
    try {
      messageType = MessageType.valueOf(message[0]);
      switch (messageType) {
        case QUERY_MAPPING_BATCH:
          int currentIndex = Byte.BYTES + Short.BYTES;
          while (currentIndex < message.length) {
            int lengthOfMapping = NumberConversion.bytes2int(message,
                    currentIndex + Byte.BYTES + Long.BYTES + Long.BYTES);
            long sender = NumberConversion.bytes2long(message,
                    currentIndex + Byte.BYTES + Long.BYTES);
            WorkerTask task = null;
            ArrayIndexOutOfBoundsException error = null;
            try {
              task = getTask(message, currentIndex + Byte.BYTES);
            } catch (ArrayIndexOutOfBoundsException e) {
              error = e;
            }
            if ((task == null) || (error != null)) {
              // if (logger != null) {
              // long receiver = NumberConversion.bytes2long(message,
              // currentIndex + Byte.BYTES);
              // logger.finest("Discarding a mapping from " + sender + " because
              // the receiving task "
              // + receiver + " is not present.");
              // }
            } else {
              task.enqueueMessage(sender, message, currentIndex, lengthOfMapping);
            }
            currentIndex += lengthOfMapping;
          }
          break;
        case QUERY_CREATED:
        case QUERY_TASK_FINISHED:
        case QUERY_TASK_FAILED:
          WorkerTask task = null;
          ArrayIndexOutOfBoundsException error = null;
          try {
            task = getTask(message, 3);
          } catch (ArrayIndexOutOfBoundsException e) {
            error = e;
          }
          if ((task == null) || (error != null)) {
            if (logger != null) {
              long receiver = NumberConversion.bytes2long(message, 3);
              logger.finest("Discarding a " + messageType.name()
                      + " message because the receiving task " + receiver + " is not present.");
            }
          } else {
            task.enqueueMessage(
                    ((long) NumberConversion.bytes2short(message, Byte.BYTES)) << (Short.SIZE
                            + Integer.SIZE),
                    message, 0, message.length);
          }
          break;
        default:
          if (logger != null) {
            logger.finer("Unknown message type received from slave: " + messageType.name());
          }
      }
    } catch (IllegalArgumentException e) {
      if (logger != null) {
        logger.finer("Unknown message type: " + message[0]);
        logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
                e);
      }
    } catch (BufferUnderflowException | IndexOutOfBoundsException e) {
      if (logger != null) {
        logger.finer("Message of type " + messageType + " is too short with only " + message.length
                + " received bytes.");
        logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
                e);
      }
    }
  }

  public void receiveLocalMessage(long sender, long receiver, byte[] message,
          int startIndexInMessage, int lengthOfMessage) {
    WorkerTask task = getTask(receiver);
    if (task == null) {
      if (logger != null) {
        logger.info("Discarding a local message because the receiving task " + receiver
                + " is not present.");
      }
    } else {
      task.enqueueMessage(sender, message, startIndexInMessage, lengthOfMessage);
    }
  }

  public void unregister(WorkerTask task) {
    byte[] id = NumberConversion.long2bytes(task.getID());
    unregisterTask(id, task);
  }

  @Override
  public void close() {
  }

  private synchronized void registerTask(byte[] id, WorkerTask task) {
    // first dimension
    int dim1Index = id[2] & 0x00_00_00_ff;
    if (taskRegistry == null) {
      taskRegistry = new WorkerTask[dim1Index + 1][][][][][];
    } else if (taskRegistry.length <= dim1Index) {
      // expand array
      WorkerTask[][][][][][] newArray = new WorkerTask[dim1Index + 1][][][][][];
      System.arraycopy(taskRegistry, 0, newArray, 0, taskRegistry.length);
      taskRegistry = newArray;
    }

    // second dimension
    int dim2Index = id[3] & 0x00_00_00_ff;
    if (taskRegistry[dim1Index] == null) {
      taskRegistry[dim1Index] = new WorkerTask[dim2Index + 1][][][][];
    } else if (taskRegistry[dim1Index].length <= dim2Index) {
      WorkerTask[][][][][] newArray = new WorkerTask[dim2Index + 1][][][][];
      System.arraycopy(taskRegistry[dim1Index], 0, newArray, 0, taskRegistry[dim1Index].length);
      taskRegistry[dim1Index] = newArray;
    }

    // third dimension
    int dim3Index = id[4] & 0x00_00_00_ff;
    if (taskRegistry[dim1Index][dim2Index] == null) {
      taskRegistry[dim1Index][dim2Index] = new WorkerTask[dim3Index + 1][][][];
    } else if (taskRegistry[dim1Index][dim2Index].length <= dim3Index) {
      WorkerTask[][][][] newArray = new WorkerTask[dim3Index + 1][][][];
      System.arraycopy(taskRegistry[dim1Index][dim2Index], 0, newArray, 0,
              taskRegistry[dim1Index][dim2Index].length);
      taskRegistry[dim1Index][dim2Index] = newArray;
    }

    // fourth dimension
    int dim4Index = id[5] & 0x00_00_00_ff;
    if (taskRegistry[dim1Index][dim2Index][dim3Index] == null) {
      taskRegistry[dim1Index][dim2Index][dim3Index] = new WorkerTask[dim4Index + 1][][];
    } else if (taskRegistry[dim1Index][dim2Index][dim3Index].length <= dim4Index) {
      WorkerTask[][][] newArray = new WorkerTask[dim4Index + 1][][];
      System.arraycopy(taskRegistry[dim1Index][dim2Index][dim3Index], 0, newArray, 0,
              taskRegistry[dim1Index][dim2Index][dim3Index].length);
      taskRegistry[dim1Index][dim2Index][dim3Index] = newArray;
    }

    // fifth dimension
    int dim5Index = id[6] & 0x00_00_00_ff;
    if (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index] == null) {
      taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index] = new WorkerTask[dim5Index + 1][];
    } else if (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index].length <= dim5Index) {
      WorkerTask[][] newArray = new WorkerTask[dim5Index + 1][];
      System.arraycopy(taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index], 0, newArray, 0,
              taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index].length);
      taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index] = newArray;
    }

    // sixth dimension
    int dim6Index = id[7] & 0x00_00_00_ff;
    if (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index] == null) {
      taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index] = new WorkerTask[dim6Index
              + 1];
    } else if (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index].length <= dim6Index) {
      WorkerTask[] newArray = new WorkerTask[dim6Index + 1];
      System.arraycopy(taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index], 0,
              newArray, 0,
              taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index].length);
      taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index] = newArray;
    }

    if (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index][dim6Index] == null) {
      taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index][dim6Index] = task;
    } else if (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index][dim6Index] != task) {
      throw new IllegalArgumentException(
              "There already exists a WorkerTask with id " + task.getID());
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("[");
    if (taskRegistry != null) {
      for (int i0 = 0; i0 < taskRegistry.length; i0++) {
        sb.append("\n\t" + i0 + "[");
        if (taskRegistry[i0] != null) {
          for (int i1 = 0; i1 < taskRegistry[i0].length; i1++) {
            sb.append("\n\t\t" + i1 + "[");
            if (taskRegistry[i0][i1] != null) {
              for (int i2 = 0; i2 < taskRegistry[i0][i1].length; i2++) {
                sb.append("\n\t\t\t" + i2 + "[");
                if (taskRegistry[i0][i1][i2] != null) {
                  for (int i3 = 0; i3 < taskRegistry[i0][i1][i2].length; i3++) {
                    sb.append("\n\t\t\t\t" + i3 + "[");
                    if (taskRegistry[i0][i1][i2][i3] != null) {
                      for (int i4 = 0; i4 < taskRegistry[i0][i1][i2][i3].length; i4++) {
                        sb.append("\n\t\t\t\t\t" + i4 + "[");
                        if (taskRegistry[i0][i1][i2][i3][i4] != null) {
                          for (int i5 = 0; i5 < taskRegistry[i0][i1][i2][i3][i4].length; i5++) {
                            if (taskRegistry[i0][i1][i2][i3][i4][i5] != null) {
                              sb.append("\n\t\t\t\t\t\t" + i5 + ": "
                                      + taskRegistry[i0][i1][i2][i3][i4][i5].getID());
                            }
                          }
                        }
                        sb.append("\n\t\t\t\t\t]" + i4);
                      }
                    }
                    sb.append("\n\t\t\t\t]" + i3);
                  }
                }
                sb.append("\n\t\t\t]" + i2);
              }
            }
            sb.append("\n\t\t]" + i1);
          }
        }
        sb.append("\n\t]" + i0);
      }
    }
    sb.append("\n]");
    return sb.toString();
  }

  private WorkerTask getTask(byte[] array, int firstIndexOfReceiverID) {
    int dim1Index = array[firstIndexOfReceiverID + 2] & 0x00_00_00_ff;
    int dim2Index = array[firstIndexOfReceiverID + 3] & 0x00_00_00_ff;
    int dim3Index = array[firstIndexOfReceiverID + 4] & 0x00_00_00_ff;
    int dim4Index = array[firstIndexOfReceiverID + 5] & 0x00_00_00_ff;
    int dim5Index = array[firstIndexOfReceiverID + 6] & 0x00_00_00_ff;
    int dim6Index = array[firstIndexOfReceiverID + 7] & 0x00_00_00_ff;

    return getTask(dim1Index, dim2Index, dim3Index, dim4Index, dim5Index, dim6Index);
  }

  private WorkerTask getTask(long receiver) {
    int dim1Index = (int) ((receiver << (2 * 8)) >>> (7 * 8));
    int dim2Index = (int) ((receiver << (3 * 8)) >>> (7 * 8));
    int dim3Index = (int) ((receiver << (4 * 8)) >>> (7 * 8));
    int dim4Index = (int) ((receiver << (5 * 8)) >>> (7 * 8));
    int dim5Index = (int) ((receiver << (6 * 8)) >>> (7 * 8));
    int dim6Index = (int) ((receiver << (7 * 8)) >>> (7 * 8));
    return getTask(dim1Index, dim2Index, dim3Index, dim4Index, dim5Index, dim6Index);
  }

  private synchronized WorkerTask getTask(int dim1Index, int dim2Index, int dim3Index,
          int dim4Index, int dim5Index, int dim6Index) {
    if ((taskRegistry == null) || (taskRegistry[dim1Index] == null)
            || (taskRegistry[dim1Index][dim2Index] == null)
            || (taskRegistry[dim1Index][dim2Index][dim3Index] == null)
            || (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index] == null)
            || (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index] == null)
            || (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index][dim6Index] == null)) {
      return null;
    } else {
      return taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index][dim6Index];
    }
  }

  public synchronized Set<WorkerTask> getAllTasksOfQuery(byte[] array, int firstIndexOfQueryID) {
    Set<WorkerTask> result = new HashSet<>();
    int dim1Index = array[firstIndexOfQueryID + 0] & 0x00_00_00_ff;
    int dim2Index = array[firstIndexOfQueryID + 1] & 0x00_00_00_ff;
    int dim3Index = array[firstIndexOfQueryID + 2] & 0x00_00_00_ff;
    int dim4Index = array[firstIndexOfQueryID + 3] & 0x00_00_00_ff;

    if ((taskRegistry == null) || (taskRegistry[dim1Index] == null)
            || (taskRegistry[dim1Index][dim2Index] == null)
            || (taskRegistry[dim1Index][dim2Index][dim3Index] == null)
            || (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index] == null)) {
      return result;
    }

    for (int dim5Index = 0; dim5Index < taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index].length; dim5Index++) {
      if (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index] != null) {
        for (int dim6Index = 0; dim6Index < taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index].length; dim6Index++) {
          if (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index][dim6Index] != null) {
            result.add(
                    taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index][dim6Index]);
          }
        }
      }
    }
    return result;
  }

  private synchronized void unregisterTask(byte[] id, WorkerTask task) {
    int dim1Index = id[2] & 0x00_00_00_ff;
    int dim2Index = id[3] & 0x00_00_00_ff;
    int dim3Index = id[4] & 0x00_00_00_ff;
    int dim4Index = id[5] & 0x00_00_00_ff;
    int dim5Index = id[6] & 0x00_00_00_ff;
    int dim6Index = id[7] & 0x00_00_00_ff;

    if ((taskRegistry == null) || (taskRegistry[dim1Index] == null)
            || (taskRegistry[dim1Index][dim2Index] == null)
            || (taskRegistry[dim1Index][dim2Index][dim3Index] == null)
            || (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index] == null)
            || (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index] == null)
            || (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index][dim6Index] == null)) {
      if (logger != null) {
        logger.finer("There is no WorkerTask registered with id " + task.getID());
      }
      return;
    }
    if (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index][dim6Index] != task) {
      throw new IllegalArgumentException("Unregistering of WorkerTask " + task.getID()
              + " not possible since there is another WorkerTask registered under this id.");
    }

    taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index][dim6Index] = null;
  }

}
