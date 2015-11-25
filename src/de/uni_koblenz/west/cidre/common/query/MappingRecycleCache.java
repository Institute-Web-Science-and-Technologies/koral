package de.uni_koblenz.west.cidre.common.query;

import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.utils.NumberConversion;
import de.uni_koblenz.west.cidre.slave.triple_store.impl.IndexType;

/**
 * <p>
 * In order to prevent the garbage collector to be executed frequently, this
 * class is used to create and release {@link Mapping} instances. A new
 * {@link Mapping} instance is only created if no previously released instance
 * is cached. In order to prevent the memory to be flooded by unused
 * {@link Mapping} instances, the number of cached instances is limited.
 * </p>
 * 
 * <p>
 * WARNING: This class is not thread safe in order to avoid synchronization
 * overhead!
 * </p>
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MappingRecycleCache {

	private final int containmentSize;

	private final Mapping[] stack;

	private int nextFreeIndex;

	public MappingRecycleCache(int size, int numberOfSlaves) {
		int containmentSize = numberOfSlaves / Byte.SIZE;
		if (numberOfSlaves % Byte.SIZE != 0) {
			containmentSize += 1;
		}
		this.containmentSize = containmentSize;
		stack = new Mapping[size];
		nextFreeIndex = 0;
	}

	private boolean isEmpty() {
		return nextFreeIndex == 0;
	}

	private boolean isFull() {
		return nextFreeIndex >= stack.length;
	}

	private void push(Mapping mapping) {
		stack[nextFreeIndex++] = mapping;
	}

	private Mapping pop() {
		Mapping mapping = stack[nextFreeIndex - 1];
		stack[nextFreeIndex - 1] = null;
		nextFreeIndex -= 1;
		return mapping;
	}

	public Mapping createMapping(TriplePattern pattern, IndexType indexType,
			byte[] triple) {
		byte[] newMapping = new byte[Byte.BYTES + Long.BYTES + Long.BYTES
				+ Integer.BYTES + Long.BYTES * pattern.getVariables().length
				+ containmentSize];
		newMapping[0] = MessageType.QUERY_MAPPING_BATCH.getValue();
		NumberConversion.int2bytes(newMapping.length, newMapping,
				Byte.BYTES + Long.BYTES + Long.BYTES);
		// set matched variables
		int insertionIndex = Byte.BYTES + Long.BYTES + Long.BYTES
				+ Integer.BYTES;
		if (pattern.isSubjectVariable()) {
			NumberConversion.long2bytes(indexType.getSubject(triple),
					newMapping, insertionIndex);
			insertionIndex += Long.BYTES;
		}
		if (pattern.isPropertyVariable()) {
			NumberConversion.long2bytes(indexType.getProperty(triple),
					newMapping, insertionIndex);
			insertionIndex += Long.BYTES;
		}
		if (pattern.isObjectVariable()) {
			NumberConversion.long2bytes(indexType.getObject(triple), newMapping,
					insertionIndex);
			insertionIndex += Long.BYTES;
		}
		if (insertionIndex < triple.length) {
			System.arraycopy(triple, 3 * Long.BYTES, newMapping, insertionIndex,
					triple.length - 3 * Long.BYTES);
		}
		return createMapping(newMapping, 0, newMapping.length);
	}

	public Mapping createMapping(byte[] byteArrayWithMapping,
			int firstIndexOfMappingInArray, int lengthOfMapping) {
		Mapping result = getMapping();
		result.set(byteArrayWithMapping, firstIndexOfMappingInArray,
				lengthOfMapping);
		return result;
	}

	public Mapping cloneMapping(Mapping mapping) {
		return createMapping(mapping.getByteArray(),
				mapping.getFirstIndexOfMappingInByteArray(),
				mapping.getLengthOfMappingInByteArray());
	}

	private Mapping getMapping() {
		Mapping result;
		if (isEmpty()) {
			result = new Mapping(containmentSize);
		} else {
			result = pop();
		}
		return result;
	}

	public void releaseMapping(Mapping mapping) {
		if (!isFull()) {
			push(mapping);
		}
	}

	public Mapping getMappingWithRestrictedVariables(Mapping mapping,
			long[] vars, long[] selectedVars) {
		Mapping result = getMapping();
		result.restrictMapping(selectedVars, mapping, vars);
		return result;
	}

	public Mapping mergeMappings(Mapping mapping1, long[] vars1,
			Mapping mapping2, long[] vars2) {
		Mapping result = getMapping();
		result.joinMappings(mapping1, vars1, mapping2, vars2);
		return result;
	}

}
