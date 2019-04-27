package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.caching;

import java.util.LinkedList;

public class ObjectRecycler<T> {

	/**
	 * Hard-referenced object that is primarily used for recycling, because it has no wrapping overhead like the
	 * LinkedList.
	 */
	private T element;

	/**
	 * List for recyclable objects that takes objects if the primary recycle container {@link #element} is already set.
	 */
	private final LinkedList<T> objects;

	private final int capacity;

	int retrievedFromList, retrievedFromReference, maxUsage;

	public ObjectRecycler(int capacity) {
		this.capacity = capacity;
		objects = new LinkedList<>();
	}

	public void dump(T object) {
		if (element == null) {
			element = object;
		} else {
			if (objects.size() < capacity) {
				objects.add(object);
				if (objects.size() > maxUsage) {
					maxUsage = objects.size() + 1;
				}
			}
		}
	}

	public T retrieve() {
		if (element != null) {
			T returnedElement = element;
			element = null;
			retrievedFromReference++;
			return returnedElement;
		} else {
			if (objects.size() > 0) {
				retrievedFromList++;
				return objects.removeFirst();
			} else {
				return null;
			}
		}
	}

	public void printStats(String owner) {
		if (retrievedFromReference > 0) {
			System.out.println("Owner " + owner + " recycled " + retrievedFromReference + " from reference and "
					+ retrievedFromList
					+ " from list and had a max usage of " + maxUsage);
			retrievedFromReference = 0;
			retrievedFromList = 0;
			maxUsage = 0;
		}
	}

}
