package de.uni_koblenz.west.cidre.common.query;

import de.uni_koblenz.west.cidre.common.executor.WorkerTask;
import de.uni_koblenz.west.cidre.common.executor.messagePassing.MessageSenderBuffer;
import de.uni_koblenz.west.cidre.common.utils.NumberConversion;

/**
 * <p>
 * This class represents one mapping produced by a query operation. The mapping
 * is stored as a byte array. If a mapping batch is received, the batch, i.e., a
 * byte array, is shared among several instances of this class.
 * </p>
 * 
 * <p>
 * To avoid garbage collection, instances of this class are reused. Therefore,
 * use {@link MappingRecycleCache} to create instances of this class and to
 * release them again.
 * </p>
 * 
 * <p>
 * Thus, instances of this class should be seen as immutable from within a
 * {@link WorkerTask}. Additionally, pointers to an instance should be removed
 * when it is sent to another task or any other operation is performed that
 * might lead to a {@link MappingRecycleCache#releaseMapping(Mapping)} call.
 * </p>
 * 
 * @see MessageSenderBuffer
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class Mapping {

	private final int containmentSize;

	/**
	 * This array consists of:
	 * <ol>
	 * <li>1 byte message type</li>
	 * <li>8 byte receiving query task id</li>
	 * <li>8 byte sender task id</li>
	 * <li>4 byte length of mapping serialization</li>
	 * <li>mapping serialization
	 * <ol>
	 * <li>8*#vars bytes of mapping</li>
	 * <li>{@link #containmentSize} bytes of containment serialization</li>
	 * </ol>
	 * </li>
	 * </ol>
	 */
	private byte[] byteArray;

	private int firstIndex;

	private int length;

	Mapping(int containmentSize) {
		this.containmentSize = containmentSize;
	}

	void set(byte[] byteArrayWithMapping, int firstIndexOfMappingInArray,
			int lengthOfMapping) {
		byteArray = byteArrayWithMapping;
		firstIndex = firstIndexOfMappingInArray;
		length = lengthOfMapping;
	}

	public void updateReceiver(long receiverTaskID) {
		NumberConversion.long2bytes(receiverTaskID, byteArray, Byte.BYTES);
	}

	public void updateSender(long senderTaskID) {
		NumberConversion.long2bytes(senderTaskID, byteArray,
				Byte.BYTES + Long.BYTES);
	}

	public void setContainmentToAll() {
		// TODO Auto-generated method stub
	}

	public void updateContainment(int currentContainingComputer,
			int nextContainingComputer) {
		// TODO Auto-generated method stub
	}

	public void restrictMapping(long[] resultVars, Mapping mapping,
			long[] vars) {
		// TODO Auto-generated method stub
	}

	public void joinMappings(Mapping mapping1, long[] vars1, Mapping mapping2,
			long[] vars2) {
		// TODO Auto-generated method stub
	}

	public byte[] getByteArray() {
		return byteArray;
	}

	/**
	 * @return index which represents the message type of this {@link Mapping}
	 */
	public int getFirstIndexOfMappingInByteArray() {
		return firstIndex;
	}

	public int getLengthOfMappingInByteArray() {
		return length;
	}

	public long getValue(long var) {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean isEmptyMapping() {
		return byteArray.length == Byte.BYTES + Long.BYTES + Long.BYTES
				+ Integer.BYTES + containmentSize;
	}

	public short getIdOfFirstComputerKnowingThisMapping() {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean isKnownByComputer(int computerId) {
		// TODO Auto-generated method stub
		return false;
	}

}
