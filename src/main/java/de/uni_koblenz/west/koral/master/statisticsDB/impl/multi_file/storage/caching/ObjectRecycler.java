package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching;

import java.util.LinkedList;

/**
 * Allows recycling of generic objects, with a limited capacity.
 *
 * @author Philipp Töws
 *
 * @param <T>
 *            The type of the objects that are recycled.
 */
public class ObjectRecycler<T> {

	private final LinkedList<T> objects;

	private final int capacity;

	int retrieved, maxUsage;

	public ObjectRecycler(int capacity) {
		this.capacity = capacity;
		objects = new LinkedList<>();
	}

	public void dump(T object) {
		if (objects.size() < capacity) {
			objects.add(object);
		}
		if (objects.size() > maxUsage) {
			maxUsage = objects.size();
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

	public void printStats(String owner) {
		if (retrieved > 0) {
			System.out.println("Owner " + owner + " recycled " + retrieved
					+ " and had a max usage of " + maxUsage);
			retrieved = 0;
			maxUsage = 0;
		}
	}

}
