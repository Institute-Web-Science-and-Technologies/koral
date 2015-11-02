package de.uni_koblenz.west.cidre.common.query;

import java.util.BitSet;

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
		Mapping result = null;
		if (isEmpty()) {
			result = new Mapping();
		} else {
			result = pop();
		}
		result.set(subject, property, object, pattern, containment);
		return result;
	}

	public void releaseMapping(Mapping mapping) {
		if (!isFull()) {
			push(mapping);
		}
	}

}
