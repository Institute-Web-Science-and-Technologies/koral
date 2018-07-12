package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.util.LinkedList;

public class ObjectRecycler<T> {

	private final LinkedList<T> objects;

	private final int capacity;

	int retrieved;

	public ObjectRecycler(int capacity) {
		this.capacity = capacity;
		objects = new LinkedList<>();
	}

	public void dump(T object) {
		if (objects.size() < capacity) {
			objects.add(object);
		}
	}

	public T retrieve() {
		if (objects.size() > 0) {
			retrieved++;
			return objects.removeFirst();
		} else {
			return null;
		}
	}

}
