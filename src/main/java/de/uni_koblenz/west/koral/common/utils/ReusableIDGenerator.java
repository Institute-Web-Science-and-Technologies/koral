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

import java.util.Arrays;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.SubbenchmarkManager;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.SubbenchmarkManager.SUBBENCHMARK_TASK;
import playground.StatisticsDBTest;

/**
 * Returns the first unused int id starting at zero.
 *
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 * @author Philipp Töws
 *
 */
public class ReusableIDGenerator {

	private static final long MAX_NUMBER_OF_IDS = Long.MAX_VALUE - 1;

	public static final int DEFAULT_INITIAL_LENGTH = 10;

	// 1000 is a good value for quickly growing arrays
	public static final int DEFAULT_EXTENSION_LENGTH = 1000;

	/**
	 * positive values represent used ids<br>
	 * negative values represent free ids
	 */
	long[] ids;

	private final int initialLength;

	/**
	 * How many bytes are allocated if the array size needs to grow
	 */
	private final int extensionLength;

	/**
	 * The currently highest id that is in use.
	 */
	private long maxId = -1;

	/**
	 * How many blocks the RLE list contains, i.e. how many fields in the ids array are filled.
	 */
	private int numberOfUsedBlocks;

	public ReusableIDGenerator() {
		this(null);
	}

	public ReusableIDGenerator(long[] ids) {
		this(ids, DEFAULT_INITIAL_LENGTH, DEFAULT_EXTENSION_LENGTH);
	}

	public ReusableIDGenerator(long[] ids, int initialLength, int extensionLength) {
		this.ids = ids;
		this.initialLength = initialLength;
		this.extensionLength = extensionLength;
		if (ids != null) {
			findMaxId();
			numberOfUsedBlocks = getNumberOfUsedBlocks();
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
		long firstFreeID = ids[0] > 0 ? ids[0] : 0;
		if (firstFreeID > ReusableIDGenerator.MAX_NUMBER_OF_IDS) {
			return -1;
		}
		return firstFreeID;
	}

	public long next() {
		long firstFreeID;
		if (ids == null) {
			long start = 0;
			if (StatisticsDBTest.SUBBENCHMARKS) {
				start = System.nanoTime();
			}
			ids = new long[initialLength];
			if (StatisticsDBTest.SUBBENCHMARKS) {
				SubbenchmarkManager.getInstance().addTime(SUBBENCHMARK_TASK.RLE_NEXT_ALLOC, System.nanoTime() - start);
			}
		}
		firstFreeID = getNextId();
		if (firstFreeID < 0) {
			throw new RuntimeException("There are no free ids available any more.");
		}
		if (ids[0] == 0) {
			// ids is empty
			ids[0]++;
			numberOfUsedBlocks = 1;
		} else if (ids[0] > 0) {
			// first block is used block
			// increase number of used ids
			ids[0]++;
			if ((ids.length >= 2) && (ids[1] < 0)) {
				// the second block is a free block
				// reduce number of free ids
				ids[1]++;
				if ((ids.length >= 3) && (ids[1] == 0) && (ids[2] > 0)) {
					// join [+x,0,+y,-z,...] to [+x+y,-z,...]
					ids[0] += ids[2];
					if (ids.length > 3) {
						long start = 0;
						if (StatisticsDBTest.SUBBENCHMARKS) {
							start = System.nanoTime();
						}
						System.arraycopy(ids, 3, ids, 1, numberOfUsedBlocks - 3);
						if (StatisticsDBTest.SUBBENCHMARKS) {
							SubbenchmarkManager.getInstance().addTime(SUBBENCHMARK_TASK.RLE_NEXT_ARRAYCOPY,
									System.nanoTime() - start);
						}
					}
					ids[numberOfUsedBlocks - 1] = 0;
					ids[numberOfUsedBlocks - 2] = 0;
					numberOfUsedBlocks -= 2;
				}
			}
		} else if (ids[0] < 0) {
			// first block is free block => ids.length>=2
			// reduce number of free ids
			ids[0]++;
			if (ids[0] < 0) {
				// [-x,+y,...] -> [+1,-x,+y,...]
				shiftArrayToRight(0, 1, SATR_CALLER.NEXT);
				ids[0] = 1;
				numberOfUsedBlocks++;
			} else if ((ids[0] == 0) && (ids.length >= 2) && (ids[1] > 0)) {
				// [0,+x,...] -> [+x+1,...]
				long start = 0;
				if (StatisticsDBTest.SUBBENCHMARKS) {
					start = System.nanoTime();
				}
				System.arraycopy(ids, 1, ids, 0, numberOfUsedBlocks - 1);
				if (StatisticsDBTest.SUBBENCHMARKS) {
					SubbenchmarkManager.getInstance().addTime(SUBBENCHMARK_TASK.RLE_NEXT_ARRAYCOPY,
							System.nanoTime() - start);
				}
				ids[numberOfUsedBlocks - 1] = 0;
				ids[0]++;
				numberOfUsedBlocks--;
			}
		}
		if (firstFreeID > maxId) {
			maxId = firstFreeID;
		}
		assert numberOfUsedBlocks == getNumberOfUsedBlocks() : "Is: " + numberOfUsedBlocks + ", exp: "
				+ getNumberOfUsedBlocks() + ". " + toString();
		return firstFreeID;
	}

	/**
	 * Only used to identify subbenchmarking tasks.
	 *
	 */
	private static enum SATR_CALLER {
		NEXT,
		RELEASE
	}

	private void shiftArrayToRight(int firstIndexToShift, int numberShifts, SATR_CALLER caller) {
		long[] src = ids;
		if (((numberOfUsedBlocks + numberShifts) - 1) >= ids.length) {
			// extend array
			long start = 0;
			if (StatisticsDBTest.SUBBENCHMARKS) {
				start = System.nanoTime();
			}
			ids = new long[ids.length + extensionLength + numberShifts];
			if (StatisticsDBTest.SUBBENCHMARKS) {
				SUBBENCHMARK_TASK task = null;
				if (caller == SATR_CALLER.NEXT) {
					task = SUBBENCHMARK_TASK.RLE_NEXT_ALLOC;
				} else if (caller == SATR_CALLER.RELEASE) {
					task = SUBBENCHMARK_TASK.RLE_RELEASE_ALLOC;
				}
				SubbenchmarkManager.getInstance().addTime(task, System.nanoTime() - start);
			}
		}
		long start = 0;
		if (StatisticsDBTest.SUBBENCHMARKS) {
			start = System.nanoTime();
		}
		System.arraycopy(src, 0, ids, 0, firstIndexToShift);
		System.arraycopy(src, firstIndexToShift, ids, firstIndexToShift + numberShifts,
				numberOfUsedBlocks - firstIndexToShift);
		if (StatisticsDBTest.SUBBENCHMARKS) {
			SUBBENCHMARK_TASK task = null;
			if (caller == SATR_CALLER.NEXT) {
				task = SUBBENCHMARK_TASK.RLE_NEXT_ARRAYCOPY;
			} else if (caller == SATR_CALLER.RELEASE) {
				task = SUBBENCHMARK_TASK.RLE_RELEASE_ARRAYCOPY;
			}
			SubbenchmarkManager.getInstance().addTime(task, System.nanoTime() - start);
		}
		for (int i = firstIndexToShift; i < (firstIndexToShift + numberShifts); i++) {
			ids[i] = 0;
		}
	}

	private int getNumberOfUsedBlocks() {
		int numberOfUsedBlocks;
		for (numberOfUsedBlocks = 0; numberOfUsedBlocks < ids.length; numberOfUsedBlocks++) {
			if (ids[numberOfUsedBlocks] == 0) {
				break;
			}
		}
		return numberOfUsedBlocks;
	}

	public void release(long idToFree) {
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
		for (deletionBlockIndex = 0; deletionBlockIndex < numberOfUsedBlocks; deletionBlockIndex++) {
			long maxCurrentId = maxPreviousId + Math.abs(ids[deletionBlockIndex]);
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
		if ((deletionBlockIndex > ids.length) || (ids[deletionBlockIndex] == 0)) {
			// the id is out of range of the given ids
			return;
		}
		if (ids[deletionBlockIndex] < 0) {
			// the id was not used
			return;
		}
		if (ids[deletionBlockIndex] == 1) {
			// the only used id in this block is freed
			if ((deletionBlockIndex == (ids.length - 1)) || (ids[deletionBlockIndex + 1] == 0)) {
				// this is the last used block
				ids[deletionBlockIndex] = 0;
				numberOfUsedBlocks--;
				if (deletionBlockIndex > 0) {
					ids[deletionBlockIndex - 1] = 0;
					numberOfUsedBlocks--;
				}
			} else if (deletionBlockIndex == 0) {
				// [1,-x,...] -> [-x-1,...]
				start = 0;
				if (StatisticsDBTest.SUBBENCHMARKS) {
					start = System.nanoTime();
				}
				System.arraycopy(ids, 1, ids, 0, numberOfUsedBlocks - 1);
				if (StatisticsDBTest.SUBBENCHMARKS) {
					SubbenchmarkManager.getInstance().addTime(SUBBENCHMARK_TASK.RLE_RELEASE_ARRAYCOPY,
							System.nanoTime() - start);
				}
				ids[0]--;
				ids[numberOfUsedBlocks - 1] = 0;
				numberOfUsedBlocks--;
			} else {
				// [..,+w,-x,1,-y,+z,...] -> [...,+w,-x-y-1,+z,...]
				int lastUsedBlockIndex = numberOfUsedBlocks - 1;
				ids[deletionBlockIndex - 1] += ids[deletionBlockIndex + 1] - 1;
				start = 0;
				if (StatisticsDBTest.SUBBENCHMARKS) {
					start = System.nanoTime();
				}
				System.arraycopy(ids, deletionBlockIndex + 2, ids, deletionBlockIndex,
						numberOfUsedBlocks - deletionBlockIndex - 2);
				if (StatisticsDBTest.SUBBENCHMARKS) {
					SubbenchmarkManager.getInstance().addTime(SUBBENCHMARK_TASK.RLE_RELEASE_ARRAYCOPY,
							System.nanoTime() - start);
				}
				ids[lastUsedBlockIndex] = 0;
				ids[lastUsedBlockIndex - 1] = 0;
				numberOfUsedBlocks -= 2;
			}
		} else if ((maxPreviousId + 1) == idToFree) {
			// the first id of a used block is freed
			if (deletionBlockIndex == 0) {
				// [+x,...]->[-1,+x-1,...]
				shiftArrayToRight(0, 1, SATR_CALLER.RELEASE);
				ids[0] = -1;
				ids[1]--;
				numberOfUsedBlocks++;
			} else {
				// [..,-x,+y,...] -> [...,-x-1,+y-1,...]
				ids[deletionBlockIndex]--;
				ids[deletionBlockIndex - 1]--;
			}
		} else if ((maxPreviousId + ids[deletionBlockIndex]) == idToFree) {
			// the last id of a used block is freed
			// [...,+x,-y,...] -> [...,+x-1,-y-1,...]
			ids[deletionBlockIndex]--;
			if (((deletionBlockIndex + 1) < ids.length) && (ids[deletionBlockIndex + 1] != 0)) {
				ids[deletionBlockIndex + 1]--;
			}
		} else {
			// the freed id is in the middle of the used block
			// [...,+x,...]->[...,+x1,-1,+x2,...]
			long x1 = idToFree - maxPreviousId - 1;
			long x2 = (maxPreviousId + ids[deletionBlockIndex]) - idToFree;
			shiftArrayToRight(deletionBlockIndex, 2, SATR_CALLER.RELEASE);
			ids[deletionBlockIndex] = x1;
			ids[deletionBlockIndex + 1] = -1;
			ids[deletionBlockIndex + 2] = x2;
			numberOfUsedBlocks += 2;
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
		assert numberOfUsedBlocks == getNumberOfUsedBlocks() : "Is: " + numberOfUsedBlocks + ", exp: "
				+ getNumberOfUsedBlocks() + ". " + toString();
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
		if ((ids == null) || (ids.length == 0)) {
			ids = new long[initialLength];
		}
		// Index of the block that contains idToSet
		int blockIndex;
		// Largest id of the block before the block that contains idToSet
		long maxPreviousId = -1;
		for (blockIndex = 0; blockIndex < numberOfUsedBlocks; blockIndex++) {
			long maxCurrentId = maxPreviousId + Math.abs(ids[blockIndex]);
			if (idToSet <= maxCurrentId) {
				break;
			} else {
				maxPreviousId = maxCurrentId;
			}
		}
		if (blockIndex == ids.length) {
			// id list is full and new id is larger
			extendIdsArray(blockIndex + 1 + 1);
		}
		if (ids[blockIndex] == 0) {
			// the id is larger than previous ids
			long unusedIds = idToSet - maxPreviousId - 1;
			if (unusedIds > 0) {
				ids[blockIndex] = -unusedIds;
				if ((blockIndex + 1) >= ids.length) {
					extendIdsArray(blockIndex + 1 + 1);
				}
				ids[blockIndex + 1] = 1;
				numberOfUsedBlocks += 2;
			} else {
				if (blockIndex > 0) {
					ids[blockIndex - 1]++;
				} else {
					ids[0] = 1;
					numberOfUsedBlocks++;
				}
			}
		} else if (ids[blockIndex] > 0) {
			// the id was already set
			return;
		} else if (ids[blockIndex] == -1) {
			// the only unused id in this block is set
			if (blockIndex == 0) {
				// [-1,x,...] -> [x+1,...]
				System.arraycopy(ids, 1, ids, 0, numberOfUsedBlocks - 1);
				ids[0]++;
				ids[numberOfUsedBlocks - 1] = 0;
				numberOfUsedBlocks--;
			} else {
				// [..,-w,+x,-1,+y,-z,...] -> [...,-w,+x+y+1,-z,...]
				int lastUsedBlockIndex = numberOfUsedBlocks - 1;
				ids[blockIndex - 1] += ids[blockIndex + 1] + 1;
				System.arraycopy(ids, blockIndex + 2, ids, blockIndex, numberOfUsedBlocks - blockIndex - 2);
				// Remove leftover data that wasn't overwritten
				ids[lastUsedBlockIndex] = 0;
				ids[lastUsedBlockIndex - 1] = 0;
				numberOfUsedBlocks -= 2;
			}
		} else if ((maxPreviousId + 1) == idToSet) {
			// the first id of an unused block is set
			if (blockIndex == 0) {
				// [-x,...]->[+1,-x+1,...]
				shiftArrayToRight(0, 1, null);
				ids[0] = 1;
				ids[1]++;
				numberOfUsedBlocks++;
			} else {
				// [..,+x,-y,...] -> [...,+x+1,-y+1,...]
				ids[blockIndex]++;
				ids[blockIndex - 1]++;
			}
		} else if ((maxPreviousId - ids[blockIndex]) == idToSet) {
			// the last id of an unused block is set
			// [...,-x,+y,...] -> [...,-x+1,+y+1,...]
			ids[blockIndex]++;
			ids[blockIndex + 1]++;
		} else {
			// the set id is in the middle of the unused block
			// [...,-x,...]->[...,-x1,+1,-x2,...]
			long x1 = idToSet - maxPreviousId - 1;
			long x2 = (maxPreviousId - ids[blockIndex]) - idToSet;
			shiftArrayToRight(blockIndex, 2, null);
			ids[blockIndex] = -x1;
			ids[blockIndex + 1] = 1;
			ids[blockIndex + 2] = -x2;
			numberOfUsedBlocks += 2;
		}
		assert numberOfUsedBlocks == getNumberOfUsedBlocks() : "Is: " + numberOfUsedBlocks + ", exp: "
				+ getNumberOfUsedBlocks() + ". " + toString();
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
		for (int i = 0; i < numberOfUsedBlocks; i++) {
			// Negative value implies the last values aren't used
			idCounter += Math.abs(ids[i]);
			if (idCounter >= id) {
				// Found the section where the desired id is included
				return ids[i] > 0;
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
		for (int i = 0; i < numberOfUsedBlocks; i++) {
			if (ids[i] < 0) {
				currentId += Math.abs(ids[i]);
			} else {
				long newId = currentId + ids[i];
				if (newId > id) {
					usedIdsCount += id - currentId;
					return usedIdsCount;
				} else {
					currentId += ids[i];
					usedIdsCount += ids[i];
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
		for (int i = 0; i < numberOfUsedBlocks; i++) {
			if (ids[i] < 0) {
				currentId += Math.abs(ids[i]);
			} else {
				long newUsedIdsCount = usedIdsCount + ids[i];
				if (newUsedIdsCount > n) {
					currentId += n - usedIdsCount;
					return currentId;
				} else {
					currentId += ids[i];
					usedIdsCount += ids[i];
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
		for (int i = 0; i < numberOfUsedBlocks; i++) {
			if (ids[i] > 0) {
				count += ids[i];
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
		for (int i = 0; i < numberOfUsedBlocks; i++) {
			count += Math.abs(ids[i]);
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
		ids = new long[initialLength];
		ids[0] = idCount;
		numberOfUsedBlocks = 1;
	}

	/**
	 * Returns a copy of the internal RLE-encoded id list. The length of the returned array is equal to the amount of
	 * used positions in the RLE list and might be different to the length of the internal ids array.
	 *
	 * @return
	 */
	public long[] getData() {
		long[] data = new long[numberOfUsedBlocks];
		System.arraycopy(ids, 0, data, 0, numberOfUsedBlocks);
		return data;
	}

	/**
	 * Resets all internal states and reinitializes ID list with empty array.
	 */
	public void clear() {
		ids = new long[initialLength];
		maxId = -1;
		numberOfUsedBlocks = 0;
	}

	public boolean isEmpty() {
		return (ids == null) || (ids.length == 0) || (ids[0] == 0);
	}

	@Override
	public String toString() {
		return (ids == null ? "[]" : Arrays.toString(ids)) + " (" + numberOfUsedBlocks + "u)";
	}

	private void extendIdsArray(int minLength) {
		long[] newIds = new long[minLength + extensionLength];
		System.arraycopy(ids, 0, newIds, 0, ids.length);
		ids = newIds;
	}

}
