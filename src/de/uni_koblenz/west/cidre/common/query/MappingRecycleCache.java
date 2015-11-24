package de.uni_koblenz.west.cidre.common.query;

import java.util.BitSet;

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

	private final Mapping[] stack;

	private int nextFreeIndex;

	public MappingRecycleCache(int size) {
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

	public Mapping createMapping(long subject, long property, long object,
			TriplePattern pattern, BitSet containment) {
		Mapping result = getMapping();
		result.set(subject, property, object, pattern, containment);
		return result;
	}

	public Mapping createMapping(byte[] byteArrayWithMapping,
			int firstIndexOfMappingInArray, int lengthOfMapping) {
		Mapping result = getMapping();
		result.set(byteArrayWithMapping, firstIndexOfMappingInArray,
				lengthOfMapping);
		return result;
	}

	private Mapping getMapping() {
		Mapping result;
		if (isEmpty()) {
			result = new Mapping();
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
			long[] selectedVars) {
		Mapping result = getMapping();
		return result.setVars(selectedVars, mapping);
	}

}
