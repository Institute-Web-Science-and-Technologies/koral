package de.uni_koblenz.west.cidre.common.executor.messagePassing;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.executor.WorkerTask;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.networManager.MessageListener;
import de.uni_koblenz.west.cidre.common.utils.NumberConversion;

/**
 * query ID and node ID should start with 0!
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
		byte[] id = ByteBuffer.allocate(8).putLong(task.getID()).array();
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
				int currentIndex = 3;
				while (currentIndex < message.length) {
					short numberOfVariables = NumberConversion
							.bytes2short(message, currentIndex + Byte.BYTES);
					long sender = NumberConversion.bytes2long(message,
							currentIndex + Byte.BYTES + Short.BYTES
									+ Long.BYTES);
					WorkerTask task = getTask(message,
							currentIndex + Byte.BYTES + Short.BYTES);
					if (task == null) {
						if (logger != null) {
							long receiver = NumberConversion.bytes2long(message,
									currentIndex + Byte.BYTES + Short.BYTES);
							logger.info(
									"Discarding a mapping because the receiving task "
											+ receiver + " is not present.");
							// TODO failure handling
						}
					} else {
						task.enqueueMessage(sender, message, currentIndex);
					}
					currentIndex += Byte.BYTES + Short.BYTES + Long.BYTES
							+ Long.BYTES + numberOfVariables * Long.BYTES;
				}
				break;
			case QUERY_TASK_FINISHED:
				WorkerTask task = getTask(message, 3);
				if (task == null) {
					if (logger != null) {
						long receiver = NumberConversion.bytes2long(message, 3);
						logger.info("Discarding a " + messageType.name()
								+ " message because the receiving task "
								+ receiver + " is not present.");
						// TODO failure handling
					}
				} else {
					task.enqueueMessage(NumberConversion.bytes2long(message,
							message.length - Long.BYTES), message, 0);
				}
				break;
			default:
				if (logger != null) {
					logger.finer("Unknown message type received from slave: "
							+ messageType.name());
				}
			}
		} catch (IllegalArgumentException e) {
			if (logger != null) {
				logger.finer("Unknown message type: " + message[0]);
				logger.throwing(e.getStackTrace()[0].getClassName(),
						e.getStackTrace()[0].getMethodName(), e);
			}
		} catch (BufferUnderflowException | IndexOutOfBoundsException e) {
			if (logger != null) {
				logger.finer("Message of type " + messageType
						+ " is too short with only " + message.length
						+ " received bytes.");
				logger.throwing(e.getStackTrace()[0].getClassName(),
						e.getStackTrace()[0].getMethodName(), e);
			}
		}
	}

	public void receiveLocalMessage(long sender, long receiver, byte[] message,
			int startIndexInMessage) {
		WorkerTask task = getTask(receiver);
		if (task == null) {
			if (logger != null) {
				logger.info(
						"Discarding a local message because the receiving task "
								+ receiver + " is not present.");
			}
		} else {
			task.enqueueMessage(sender, message, startIndexInMessage);
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
			WorkerTask[][][][][][] newArray = new WorkerTask[dim1Index
					+ 1][][][][][];
			System.arraycopy(taskRegistry, 0, newArray, 0, taskRegistry.length);
			taskRegistry = newArray;
		}

		// second dimension
		int dim2Index = id[3] & 0x00_00_00_ff;
		if (taskRegistry[dim1Index] == null) {
			taskRegistry[dim1Index] = new WorkerTask[dim2Index + 1][][][][];
		} else if (taskRegistry[dim1Index].length <= dim2Index) {
			WorkerTask[][][][][] newArray = new WorkerTask[dim2Index
					+ 1][][][][];
			System.arraycopy(taskRegistry[dim1Index], 0, newArray, 0,
					taskRegistry[dim1Index].length);
			taskRegistry[dim1Index] = newArray;
		}

		// third dimension
		int dim3Index = id[4] & 0x00_00_00_ff;
		if (taskRegistry[dim1Index][dim2Index] == null) {
			taskRegistry[dim1Index][dim2Index] = new WorkerTask[dim3Index
					+ 1][][][];
		} else if (taskRegistry[dim1Index][dim2Index].length <= dim3Index) {
			WorkerTask[][][][] newArray = new WorkerTask[dim3Index + 1][][][];
			System.arraycopy(taskRegistry[dim1Index][dim2Index], 0, newArray, 0,
					taskRegistry[dim1Index][dim2Index].length);
			taskRegistry[dim1Index][dim2Index] = newArray;
		}

		// fourth dimension
		int dim4Index = id[5] & 0x00_00_00_ff;
		if (taskRegistry[dim1Index][dim2Index][dim3Index] == null) {
			taskRegistry[dim1Index][dim2Index][dim3Index] = new WorkerTask[dim4Index
					+ 1][][];
		} else if (taskRegistry[dim1Index][dim2Index][dim3Index].length <= dim4Index) {
			WorkerTask[][][] newArray = new WorkerTask[dim4Index + 1][][];
			System.arraycopy(taskRegistry[dim1Index][dim2Index][dim3Index], 0,
					newArray, 0,
					taskRegistry[dim1Index][dim2Index][dim3Index].length);
			taskRegistry[dim1Index][dim2Index][dim3Index] = newArray;
		}

		// fifth dimension
		int dim5Index = id[6] & 0x00_00_00_ff;
		if (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index] == null) {
			taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index] = new WorkerTask[dim5Index
					+ 1][];
		} else if (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index].length <= dim5Index) {
			WorkerTask[][] newArray = new WorkerTask[dim5Index + 1][];
			System.arraycopy(
					taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index], 0,
					newArray, 0,
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
			System.arraycopy(
					taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index],
					0, newArray, 0,
					taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index].length);
			taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index] = newArray;
		}

		if (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index][dim6Index] == null) {
			taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index][dim6Index] = task;
		} else if (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index][dim6Index] != task) {
			throw new IllegalArgumentException(
					"There already exists a WorkerTask with id "
							+ task.getID());
		}
	}

	private WorkerTask getTask(byte[] array, int firstIndexOfReceiverID) {
		int dim1Index = array[firstIndexOfReceiverID + 2] & 0x00_00_00_ff;
		int dim2Index = array[firstIndexOfReceiverID + 3] & 0x00_00_00_ff;
		int dim3Index = array[firstIndexOfReceiverID + 4] & 0x00_00_00_ff;
		int dim4Index = array[firstIndexOfReceiverID + 5] & 0x00_00_00_ff;
		int dim5Index = array[firstIndexOfReceiverID + 6] & 0x00_00_00_ff;
		int dim6Index = array[firstIndexOfReceiverID + 7] & 0x00_00_00_ff;

		return getTask(dim1Index, dim2Index, dim3Index, dim4Index, dim5Index,
				dim6Index);
	}

	private WorkerTask getTask(long receiver) {
		int dim1Index = (int) ((receiver << (2 * 8)) >>> (7 * 8));
		int dim2Index = (int) ((receiver << (3 * 8)) >>> (7 * 8));
		int dim3Index = (int) ((receiver << (4 * 8)) >>> (7 * 8));
		int dim4Index = (int) ((receiver << (5 * 8)) >>> (7 * 8));
		int dim5Index = (int) ((receiver << (6 * 8)) >>> (7 * 8));
		int dim6Index = (int) ((receiver << (7 * 8)) >>> (7 * 8));
		return getTask(dim1Index, dim2Index, dim3Index, dim4Index, dim5Index,
				dim6Index);
	}

	private synchronized WorkerTask getTask(int dim1Index, int dim2Index,
			int dim3Index, int dim4Index, int dim5Index, int dim6Index) {
		if (taskRegistry[dim1Index] == null
				|| taskRegistry[dim1Index][dim2Index] == null
				|| taskRegistry[dim1Index][dim2Index][dim3Index] == null
				|| taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index] == null
				|| taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index] == null
				|| taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index][dim6Index] == null) {
			return null;
		} else {
			return taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index][dim6Index];
		}
	}

	public synchronized Set<WorkerTask> getAllTasksOfQuery(byte[] array,
			int firstIndexOfQueryID) {
		Set<WorkerTask> result = new HashSet<>();
		int dim1Index = array[firstIndexOfQueryID + 0] & 0x00_00_00_ff;
		int dim2Index = array[firstIndexOfQueryID + 1] & 0x00_00_00_ff;
		int dim3Index = array[firstIndexOfQueryID + 2] & 0x00_00_00_ff;
		int dim4Index = array[firstIndexOfQueryID + 3] & 0x00_00_00_ff;

		if (taskRegistry[dim1Index] == null
				|| taskRegistry[dim1Index][dim2Index] == null
				|| taskRegistry[dim1Index][dim2Index][dim3Index] == null
				|| taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index] == null) {
			return result;
		}

		for (int dim5Index = 0; dim5Index <= taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index].length; dim5Index++) {
			if (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index] != null) {
				for (int dim6Index = 0; dim6Index <= taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index].length; dim6Index++) {
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

		if (taskRegistry[dim1Index] == null
				|| taskRegistry[dim1Index][dim2Index] == null
				|| taskRegistry[dim1Index][dim2Index][dim3Index] == null
				|| taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index] == null
				|| taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index] == null
				|| taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index][dim6Index] == null) {
			if (logger != null) {
				logger.finer("There is no WorkerTask registered with id "
						+ task.getID());
			}
			return;
		}
		if (taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index][dim6Index] != task) {
			throw new IllegalArgumentException("Unregistering of WorkerTask "
					+ task.getID()
					+ " not possible since there is another WorkerTask registered under this id.");
		}

		taskRegistry[dim1Index][dim2Index][dim3Index][dim4Index][dim5Index][dim6Index] = null;
	}

}
