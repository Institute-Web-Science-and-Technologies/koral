package de.uni_koblenz.west.koral.common.utils;

public class RandomAccessRLEList extends ReusableIDGenerator {

	public RandomAccessRLEList() {
	}

	public RandomAccessRLEList(long[] ids) {
		super(ids);
	}

	@Override
	public long getNextId() {
		throw new UnsupportedOperationException("Can't retrieve next id on a random access RLE list");
	}

	@Override
	public long next() {
		throw new UnsupportedOperationException("Can't retrieve next id on a random access RLE list");
	}

	/**
	 * Sets the given id to used. Must be greater than the current maximum id.
	 *
	 * @param id
	 */
	public void set(long id) {
		if (ids == null) {
			ids = new long[10];
		}
		long currentId = 0;
		for (int i = 0; i < ids.length; i++) {
			if (ids[i] != 0) {
				currentId += Math.abs(ids[i]);
			} else {
				long unusedIds = id - currentId;
				if (unusedIds > 0) {
					// 1 offset for i because it's an index and we want the length, and 1 byte for negative and positive
					// entry each
					int neededLength = (i + 1) + 2;
					if (ids.length < neededLength) {
						long[] newIds = new long[neededLength + 10];
						System.arraycopy(ids, 0, newIds, 0, ids.length);
						ids = newIds;
					}
					ids[i] = -unusedIds;
					ids[i + 1] = 1;
				} else {
					if (i > 0) {
						ids[i - 1]++;
					} else {
						ids[0] = 1;
					}
				}
				break;
			}
		}
	}

}
