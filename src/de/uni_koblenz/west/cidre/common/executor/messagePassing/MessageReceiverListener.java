package de.uni_koblenz.west.cidre.common.executor.messagePassing;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.executor.WorkerTask;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.networManager.MessageListener;

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
			case QUERY_TASK_FINISHED:
				WorkerTask task = getTask(message, 3);
				if (task == null) {
					if (logger != null) {
						long receiver = ByteBuffer.wrap(message, 3, 8)
								.getLong();
						logger.info("Discarding a " + messageType.name()
								+ " message because the receiving task "
								+ receiver + " is not present.");
					}
				} else {
					task.enqueueMessage(message);
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

	public void unregister(WorkerTask task) {
		byte[] id = ByteBuffer.allocate(8).putLong(task.getID()).array();
		unregisterTask(id, task);
	}

	@Override
	public void close() {
	}

	private synchronized void registerTask(byte[] id, WorkerTask task) {
		// first dimension
		int dim1Index = id[2] < 0 ? 256 + id[2] : id[2];
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
		int dim2Index = id[3] < 0 ? 256 + id[3] : id[3];
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
		int dim3Index = id[4] < 0 ? 256 + id[4] : id[4];
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
		int dim4Index = id[5] < 0 ? 256 + id[5] : id[5];
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
		int dim5Index = id[6] < 0 ? 256 + id[6] : id[6];
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
		int dim6Index = id[7] < 0 ? 256 + id[7] : id[7];
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

	private synchronized WorkerTask getTask(byte[] array,
			int firstIndexOfReceiverID) {
		int dim1Index = array[firstIndexOfReceiverID + 2] < 0
				? 256 + array[firstIndexOfReceiverID + 2]
				: array[firstIndexOfReceiverID + 2];
		int dim2Index = array[firstIndexOfReceiverID + 3] < 0
				? 256 + array[firstIndexOfReceiverID + 3]
				: array[firstIndexOfReceiverID + 3];
		int dim3Index = array[firstIndexOfReceiverID + 4] < 0
				? 256 + array[firstIndexOfReceiverID + 4]
				: array[firstIndexOfReceiverID + 4];
		int dim4Index = array[firstIndexOfReceiverID + 5] < 0
				? 256 + array[firstIndexOfReceiverID + 5]
				: array[firstIndexOfReceiverID + 5];
		int dim5Index = array[firstIndexOfReceiverID + 6] < 0
				? 256 + array[firstIndexOfReceiverID + 6]
				: array[firstIndexOfReceiverID + 6];
		int dim6Index = array[firstIndexOfReceiverID + 7] < 0
				? 256 + array[firstIndexOfReceiverID + 7]
				: array[firstIndexOfReceiverID + 7];

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
		int dim1Index = array[firstIndexOfQueryID + 0] < 0
				? 256 + array[firstIndexOfQueryID + 0]
				: array[firstIndexOfQueryID + 0];
		int dim2Index = array[firstIndexOfQueryID + 1] < 0
				? 256 + array[firstIndexOfQueryID + 1]
				: array[firstIndexOfQueryID + 1];
		int dim3Index = array[firstIndexOfQueryID + 2] < 0
				? 256 + array[firstIndexOfQueryID + 2]
				: array[firstIndexOfQueryID + 2];
		int dim4Index = array[firstIndexOfQueryID + 3] < 0
				? 256 + array[firstIndexOfQueryID + 3]
				: array[firstIndexOfQueryID + 3];

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
		int dim1Index = id[2] < 0 ? 256 + id[2] : id[2];
		int dim2Index = id[3] < 0 ? 256 + id[3] : id[3];
		int dim3Index = id[4] < 0 ? 256 + id[4] : id[4];
		int dim4Index = id[5] < 0 ? 256 + id[5] : id[5];
		int dim5Index = id[6] < 0 ? 256 + id[6] : id[6];
		int dim6Index = id[7] < 0 ? 256 + id[7] : id[7];

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
