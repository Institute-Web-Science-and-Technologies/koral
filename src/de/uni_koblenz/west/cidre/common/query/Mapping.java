package de.uni_koblenz.west.cidre.common.query;

import java.util.BitSet;

import de.uni_koblenz.west.cidre.common.executor.WorkerTask;
import de.uni_koblenz.west.cidre.common.executor.messagePassing.MessageSenderBuffer;

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

	// TODO (1 byte message type, 8 byte receiving query task id, 8 byte sender
	// task id, 4 byte length of mapping serialization, mapping serialization)

	Mapping() {
	}

	void set(long subject, long property, long object, TriplePattern pattern,
			BitSet containment) {
		// TODO Auto-generated method stub

	}

	public void set(byte[] byteArrayWithMapping, int firstIndexOfMappingInArray,
			int lengthOfMapping) {
		// TODO Auto-generated method stub

	}

	public void updateReceiver(long receiverTaskID) {
		// TODO Auto-generated method stub

	}

	public void updateSender(long senderTaskID) {
		// TODO Auto-generated method stub

	}

	public byte[] getByteArray() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @return index which represents the message type of this {@link Mapping}
	 */
	public int getFirstIndexOfMappingInByteArray() {
		// TODO Auto-generated method stub
		return 0;
	}

	public int getLengthOfMappingInByteArray() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getValue(long var) {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean isEmptyMapping() {
		// TODO Auto-generated method stub
		return false;
	}

	public short getIdOfFirstComputerKnowingThisMapping() {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean isKnownByComputer(int owner) {
		// TODO Auto-generated method stub
		return false;
	}

	public Mapping restrictMapping(long[] resultVars, Mapping mapping) {
		// TODO Auto-generated method stub
		return null;
	}

	public Mapping joinMappings(Mapping mapping1, Mapping mapping2) {
		// TODO Auto-generated method stub
		return null;
	}

}
