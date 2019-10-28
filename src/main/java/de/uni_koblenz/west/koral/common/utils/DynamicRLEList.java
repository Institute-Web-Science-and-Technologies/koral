/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License
 * along with Koral.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
package de.uni_koblenz.west.koral.common.utils;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.SubbenchmarkManager;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.SubbenchmarkManager.SUBBENCHMARK_TASK;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.DynamicNumberArray;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.DynamicNumberArray.Caller;
import playground.StatisticsDBTest;

/**
 * Attempt to replace RLE list with arrays with fixed per-element-bits with a dynamic and more space-efficient
 * {@link DynamicNumberArray} implementation.
 *
 * @author Philipp Töws
 */
public class DynamicRLEList {

	private static final long MAX_NUMBER_OF_IDS = Long.MAX_VALUE - 1;

	public static final int DEFAULT_INITIAL_LENGTH = 10;

	// 1000 is a good value for quickly growing arrays
	public static final int DEFAULT_EXTENSION_LENGTH = 1000;

	/**
	 * positive values represent used ids<br>
	 * negative values represent free ids
	 */
	private final DynamicNumberArray ids;

	/**
	 * The currently highest id that is in use.
	 */
	private long maxId = -1;

	public DynamicRLEList() {
		this(null);
	}

	public DynamicRLEList(byte[] ids) {
		this(ids, 1, DEFAULT_INITIAL_LENGTH, DEFAULT_EXTENSION_LENGTH);
	}

	/**
	 *
	 * @param ids
	 * @param valueSize
	 * @param initialLength
	 *            If an array is already given, this value is still used to reinitialize it after e.g. clearing
	 * @param extensionLength
	 */
	public DynamicRLEList(byte[] ids, int valueSize, int initialLength, int extensionLength) {
		if (ids != null) {
//			this.ids = new DynamicNumberArrayAccessor(ids, valueSize, extensionLength);
//			findMaxId();
			throw new UnsupportedOperationException("TODO");
		} else {
			this.ids = new DynamicNumberArray(initialLength, 1, extensionLength);
		}
	}

	/**
	 *
	 * @return Next ID without changing internal list. Returns -1 if there are no free ids available.
	 */
	public long getNextId() {
		if (ids == null) {
			return 0;
		}
		long firstFreeID = ids.get(0) > 0 ? ids.get(0) : 0;
		if (firstFreeID > DynamicRLEList.MAX_NUMBER_OF_IDS) {
			return -1;
		}
		return firstFreeID;
	}

	public long next() {
		Caller caller = Caller.NEXT;
		long firstFreeID = getNextId();
		if (firstFreeID < 0) {
			throw new RuntimeException("There are no free ids available any more.");
		}
		if (ids.get(0) == 0) {
			// ids is empty
			ids.inc(0, caller);
		} else if (ids.get(0) > 0) {
			// first block is used block
			// increase number of used ids
			ids.inc(0, caller);
			if ((ids.capacity() >= 2) && (ids.get(1) < 0)) {
				// the second block is a free block
				// reduce number of free ids
				ids.inc(1, caller);
				if ((ids.capacity() >= 3) && (ids.get(1) == 0) && (ids.get(2) > 0)) {
					// join [+x,0,+y,-z,...] to [+x+y,-z,...]
					ids.set(0, ids.get(0) + ids.get(2), caller);
					if (ids.capacity() > 3) {
						ids.move(3, 1, caller);
					}
				}
			}
		} else if (ids.get(0) < 0) {
			// first block is free block => ids.length>=2
			// reduce number of free ids
			ids.inc(0, caller);
			if (ids.get(0) < 0) {
				// [-x,+y,...] -> [+1,-x,+y,...]
				ids.insertGap(0, 1, caller);
				ids.set(0, 1, caller);
			} else if ((ids.get(0) == 0) && (ids.capacity() >= 2) && (ids.get(1) > 0)) {
				// [0,+x,...] -> [+x+1,...]
				ids.move(1, 0, caller);
				ids.inc(0, caller);
			}
		}
		if (firstFreeID > maxId) {
			maxId = firstFreeID;
		}
		return firstFreeID;
	}

	public void release(long idToFree) {
		Caller caller = Caller.RELEASE;
		if (isEmpty()) {
			return;
		}
		long start = 0;
		if (StatisticsDBTest.SUBBENCHMARKS) {
			start = System.nanoTime();
		}
		// find block to delete from
		int deletionBlockIndex;
		long maxPreviousId = -1;
		for (deletionBlockIndex = 0; deletionBlockIndex <= ids.getLastUsedIndex(); deletionBlockIndex++) {
			long maxCurrentId = maxPreviousId + Math.abs(ids.get(deletionBlockIndex));
			if (idToFree <= maxCurrentId) {
				break;
			} else {
				maxPreviousId = maxCurrentId;
			}
		}
		if (StatisticsDBTest.SUBBENCHMARKS) {
			SubbenchmarkManager.getInstance().addTime(SUBBENCHMARK_TASK.RLE_RELEASE_FINDBLOCK,
					System.nanoTime() - start);
		}
		if ((deletionBlockIndex > ids.capacity()) || (ids.get(deletionBlockIndex) == 0)) {
			// the id is out of range of the given ids
			return;
		}
		if (ids.get(deletionBlockIndex) < 0) {
			// the id was not used
			return;
		}
		if (ids.get(deletionBlockIndex) == 1) {
			// the only used id in this block is freed
			if ((deletionBlockIndex == (ids.capacity() - 1)) || (ids.get(deletionBlockIndex + 1) == 0)) {
				// this is the last used block
				ids.set(deletionBlockIndex, 0, caller);
				if (deletionBlockIndex > 0) {
					ids.set(deletionBlockIndex - 1, 0, caller);
				}
			} else if (deletionBlockIndex == 0) {
				// [1,-x,...] -> [-x-1,...]
				ids.move(1, 0, caller);
				ids.dec(0, caller);
			} else {
				// [..,+w,-x,1,-y,+z,...] -> [...,+w,-x-y-1,+z,...]
				ids.set(deletionBlockIndex - 1,
						(ids.get(deletionBlockIndex - 1) + ids.get(deletionBlockIndex + 1)) - 1, caller);
				ids.move(deletionBlockIndex + 2, deletionBlockIndex, caller);
			}
		} else if ((maxPreviousId + 1) == idToFree) {
			// the first id of a used block is freed
			if (deletionBlockIndex == 0) {
				// [+x,...]->[-1,+x-1,...]
				ids.insertGap(0, 1, caller);
				ids.set(0, -1, caller);
				ids.dec(1, caller);
			} else {
				// [..,-x,+y,...] -> [...,-x-1,+y-1,...]
				ids.dec(deletionBlockIndex, caller);
				ids.dec(deletionBlockIndex - 1, caller);
			}
		} else if ((maxPreviousId + ids.get(deletionBlockIndex)) == idToFree) {
			// the last id of a used block is freed
			// [...,+x,-y,...] -> [...,+x-1,-y-1,...]
			ids.dec(deletionBlockIndex, caller);
			if (((deletionBlockIndex + 1) < ids.capacity()) && (ids.get(deletionBlockIndex + 1) != 0)) {
				ids.dec(deletionBlockIndex + 1, caller);
			}
		} else {
			// the freed id is in the middle of the used block
			// [...,+x,...]->[...,+x1,-1,+x2,...]
			long x1 = idToFree - maxPreviousId - 1;
			long x2 = (maxPreviousId + ids.get(deletionBlockIndex)) - idToFree;
			ids.insertGap(deletionBlockIndex, 2, caller);
			ids.set(deletionBlockIndex, x1, caller);
			ids.set(deletionBlockIndex + 1, -1, caller);
			ids.set(deletionBlockIndex + 2, x2, caller);
		}
		if (idToFree == maxId) {
			start = 0;
			if (StatisticsDBTest.SUBBENCHMARKS) {
				start = System.nanoTime();
			}
			findMaxId();
			if (StatisticsDBTest.SUBBENCHMARKS) {
				SubbenchmarkManager.getInstance().addTime(SUBBENCHMARK_TASK.RLE_RELEASE_FINDMAXID,
						System.nanoTime() - start);
			}
		}
	}

	/**
	 * Sets the given id to used.
	 *
	 * @param id
	 */
	public void set(long idToSet) {
		if (StatisticsDBTest.SUBBENCHMARKS) {
			throw new UnsupportedOperationException("Subbenchmarks not yet implemented for set() method in RLE list");
		}
		Caller caller = null;
		// Index of the block that contains idToSet
		int blockIndex;
		// Largest id of the block before the block that contains idToSet
		long maxPreviousId = -1;
		for (blockIndex = 0; blockIndex <= ids.getLastUsedIndex(); blockIndex++) {
			long maxCurrentId = maxPreviousId + Math.abs(ids.get(blockIndex));
			if (idToSet <= maxCurrentId) {
				break;
			} else {
				maxPreviousId = maxCurrentId;
			}
		}
		if (ids.get(blockIndex) == 0) {
			// the id is larger than previous ids
			long unusedIds = idToSet - maxPreviousId - 1;
			if (unusedIds > 0) {
				ids.set(blockIndex, -unusedIds, caller);
				ids.set(blockIndex + 1, 1, caller);
			} else {
				if (blockIndex > 0) {
					ids.inc(blockIndex - 1, caller);
				} else {
					ids.set(0, 1, caller);
				}
			}
		} else if (ids.get(blockIndex) > 0) {
			// the id was already set
			return;
		} else if (ids.get(blockIndex) == -1) {
			// the only unused id in this block is set
			if (blockIndex == 0) {
				// [-1,x,...] -> [x+1,...]
				ids.move(1, 0, caller);
				ids.inc(0, caller);
			} else {
				// [..,-w,+x,-1,+y,-z,...] -> [...,-w,+x+y+1,-z,...]
				ids.set(blockIndex - 1, ids.get(blockIndex - 1) + ids.get(blockIndex + 1) + 1, caller);
				ids.move(blockIndex + 2, blockIndex, caller);
			}
		} else if ((maxPreviousId + 1) == idToSet) {
			// the first id of an unused block is set
			if (blockIndex == 0) {
				// [-x,...]->[+1,-x+1,...]
				ids.insertGap(0, 1, caller);
				ids.set(0, 1, caller);
				ids.inc(1, caller);
			} else {
				// [..,+x,-y,...] -> [...,+x+1,-y+1,...]
				ids.inc(blockIndex, caller);
				ids.inc(blockIndex - 1, caller);
			}
		} else if ((maxPreviousId - ids.get(blockIndex)) == idToSet) {
			// the last id of an unused block is set
			// [...,-x,+y,...] -> [...,-x+1,+y+1,...]
			ids.inc(blockIndex, caller);
			ids.inc(blockIndex + 1, caller);
		} else {
			// the set id is in the middle of the unused block
			// [...,-x,...]->[...,-x1,+1,-x2,...]
			long x1 = idToSet - maxPreviousId - 1;
			long x2 = (maxPreviousId - ids.get(blockIndex)) - idToSet;
			ids.insertGap(blockIndex, 2, caller);
			ids.set(blockIndex, -x1, caller);
			ids.set(blockIndex + 1, 1, caller);
			ids.set(blockIndex + 2, -x2, caller);
		}
	}

	public boolean isUsed(long id) {
		if (isEmpty() || (id < 0)) {
			return false;
		}
		if (id > maxId) {
			return false;
		}
		// Refers to the last id that the RLE list currently refers to
		long idCounter = -1;
		for (int i = 0; i <= ids.getLastUsedIndex(); i++) {
			// Negative value implies the last values aren't used
			idCounter += Math.abs(ids.get(i));
			if (idCounter >= id) {
				// Found the section where the desired id is included
				return ids.get(i) > 0;
			}
		}
		return false;
	}

	/**
	 * Returns the amount of used ids before the given id. It is assumed that the id is currently in use (no exceptions
	 * are thrown if not). The given id itself is not counted. Can be viewed as translating a total index to an index of
	 * the used subset. Pretty much the opposite of {@link #positionOf(long)}.
	 *
	 * @param id
	 * @return
	 */
	public long usedIdsBefore(long id) {
		if (id < 0) {
			throw new IllegalArgumentException("Id can't be negative");
		}
		if (ids == null) {
			return 0;
		}
		long usedIdsCount = 0;
		long currentId = 0;
		for (int i = 0; i <= ids.getLastUsedIndex(); i++) {
			if (ids.get(i) < 0) {
				currentId += Math.abs(ids.get(i));
			} else {
				long newId = currentId + ids.get(i);
				if (newId > id) {
					usedIdsCount += id - currentId;
					return usedIdsCount;
				} else {
					currentId += ids.get(i);
					usedIdsCount += ids.get(i);
				}
			}
		}
		if (currentId <= id) {
			throw new IllegalArgumentException("Given id " + id + " is not in range of RLE");
		}
		return usedIdsCount;
	}

	/**
	 * Returns the (n+1)th used id. Can be viewed as translating an index of the used subset to the total index. Pretty
	 * much the opposite of {@link #usedIdsBefore(long)}.
	 *
	 * @param n
	 * @return
	 */
	public long positionOf(long n) {
		if (n < 0) {
			throw new IllegalArgumentException("n can't be null");
		}
		if (ids == null) {
			throw new NullPointerException("No ids used yet");
		}
		long usedIdsCount = 0;
		long currentId = 0;
		for (int i = 0; i <= ids.getLastUsedIndex(); i++) {
			if (ids.get(i) < 0) {
				currentId += Math.abs(ids.get(i));
			} else {
				long newUsedIdsCount = usedIdsCount + ids.get(i);
				if (newUsedIdsCount > n) {
					currentId += n - usedIdsCount;
					return currentId;
				} else {
					currentId += ids.get(i);
					usedIdsCount += ids.get(i);
				}
			}
		}
		if (usedIdsCount < (n + 1)) {
			throw new IllegalArgumentException("RLE list has less than the required " + n + " ids in use.");
		}
		return currentId;
	}

	/**
	 * How many IDs are currently in use.
	 *
	 * @return
	 */
	public long usedIdsCount() {
		if (isEmpty()) {
			return 0;
		}
		long count = 0;
		for (int i = 0; i <= ids.getLastUsedIndex(); i++) {
			if (ids.get(i) > 0) {
				count += ids.get(i);
			}
		}
		return count;
	}

	/**
	 *
	 * @return The maximum ID that is currently in use, and -1 if there are no IDs in use.
	 */
	public long getMaxId() {
		return maxId;
	}

	/**
	 * Finds the maximum ID that is currently in use by iterating the ID list and assigns this value to {@link #maxId}.
	 */
	private void findMaxId() {
		if (isEmpty()) {
			maxId = -1;
		}
		long count = 0;
		for (int i = 0; i <= ids.getLastUsedIndex(); i++) {
			count += Math.abs(ids.get(i));
		}
		// IDs start at zero
		maxId = count - 1;
	}

	/**
	 * Defrags the internal list by simply setting only the first n ids as used, where n is the amount of currently used
	 * ids.
	 */
	public void defrag() {
		long idCount = usedIdsCount();
		ids.reset();
		ids.set(0, idCount, null);
	}

	/**
	 * Returns a copy of the internal RLE-encoded id list as byte array. One value of the RLE list might consist of
	 * multiple byte fields.
	 *
	 * @return
	 */
	public byte[] getData() {
		return ids.getData();
	}

	/**
	 * Resets all internal states and reinitializes ID list with empty array.
	 */
	public void clear() {
		ids.reset();
		maxId = -1;
	}

	public boolean isEmpty() {
		return (ids == null) || (ids.capacity() == 0) || (ids.get(0) == 0);
	}

	@Override
	public String toString() {
		return ids.toString();
	}

}
