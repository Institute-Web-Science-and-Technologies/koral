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

/**
 * Returns the first unused int id.
 *
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class ReusableIDGenerator {

	private static final long MAX_NUMBER_OF_IDS = Long.MAX_VALUE - 1;

	/**
	 * positive values represent used ids<br>
	 * negative values represent free ids
	 */
	long[] ids;

	public ReusableIDGenerator() {
	}

	public ReusableIDGenerator(long[] ids) {
		this.ids = ids;
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
			ids = new long[10];
			ids[0] = 1;
			return 0;
		} else {
			firstFreeID = getNextId();
		}
		if (firstFreeID < 0) {
			throw new RuntimeException("There are no free ids available any more.");
		}
		if (ids[0] == 0) {
			// ids is empty
			ids[0]++;
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
						System.arraycopy(ids, 3, ids, 1, ids.length - 3);
					}
					ids[ids.length - 1] = 0;
					ids[ids.length - 2] = 0;
				}
			}
		} else if (ids[0] < 0) {
			// first block is free block => ids.length>=2
			// reduce number of free ids
			ids[0]++;
			if (ids[0] < 0) {
				// [-x,+y,...] -> [+1,-x,+y,...]
				shiftArrayToRight(0, 1);
				ids[0] = 1;
			} else if ((ids[0] == 0) && (ids.length >= 2) && (ids[1] > 0)) {
				// [0,+x,...] -> [+x+1,...]
				System.arraycopy(ids, 1, ids, 0, ids.length - 1);
				ids[ids.length - 1] = 0;
				ids[0]++;
			}
		}
		return firstFreeID;
	}

	private void shiftArrayToRight(int firstIndexToShift, int numberShifts) {
		long[] src = ids;
		int numberOfUsedBlocks = getNumberOfUsedBlocks();
		if (((numberOfUsedBlocks + numberShifts) - 1) >= ids.length) {
			// extend array
			ids = new long[ids.length + 10 + numberShifts];
		}
		System.arraycopy(src, 0, ids, 0, firstIndexToShift);
		System.arraycopy(src, firstIndexToShift, ids, firstIndexToShift + numberShifts,
				numberOfUsedBlocks - firstIndexToShift);
		for (int i = firstIndexToShift; i < (firstIndexToShift + numberShifts); i++) {
			ids[i] = 0;
		}
	}

	private int getNumberOfUsedBlocks() {
		int numberOfUsedBlocks;
		for (numberOfUsedBlocks = 1; numberOfUsedBlocks < ids.length; numberOfUsedBlocks++) {
			if (ids[numberOfUsedBlocks] == 0) {
				break;
			}
		}
		return numberOfUsedBlocks;
	}

	public void release(long idToFree) {
		if (ids.length == 0) {
			return;
		}
		if (ids[0] == 0) {
			// no ids are used
			return;
		}
		// find block to delete from
		int deletionBlockIndex;
		long maxPreviousId = -1;
		for (deletionBlockIndex = 0; (deletionBlockIndex < ids.length)
				&& (ids[deletionBlockIndex] != 0); deletionBlockIndex++) {
			long maxCurrentId = ids[deletionBlockIndex] < 0 ? maxPreviousId - ids[deletionBlockIndex]
					: maxPreviousId + ids[deletionBlockIndex];
			if (idToFree <= maxCurrentId) {
				break;
			} else {
				maxPreviousId = maxCurrentId;
			}
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
				if (deletionBlockIndex > 0) {
					ids[deletionBlockIndex - 1] = 0;
				}
			} else if (deletionBlockIndex == 0) {
				// [1,-x,...] -> [-x-1,...]
				System.arraycopy(ids, 1, ids, 0, ids.length - 1);
				ids[0]--;
				ids[ids.length - 1] = 0;
			} else {
				// [..,+w,-x,1,-y,+z,...] -> [...,+w,-x-y-1,+z,...]
				int lastUsedBlockIndex = getNumberOfUsedBlocks() - 1;
				ids[deletionBlockIndex - 1] += ids[deletionBlockIndex + 1] - 1;
				System.arraycopy(ids, deletionBlockIndex + 2, ids, deletionBlockIndex,
						ids.length - deletionBlockIndex - 2);
				ids[lastUsedBlockIndex] = 0;
				ids[lastUsedBlockIndex - 1] = 0;
			}
		} else if ((maxPreviousId + 1) == idToFree) {
			// the first id of a used block is freed
			if (deletionBlockIndex == 0) {
				// [+x,...]->[-1,+x-1,...]
				shiftArrayToRight(0, 1);
				ids[0] = -1;
				ids[1]--;
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
			shiftArrayToRight(deletionBlockIndex, 2);
			ids[deletionBlockIndex] = x1;
			ids[deletionBlockIndex + 1] = -1;
			ids[deletionBlockIndex + 2] = x2;
		}
	}

	/**
	 * Sets the given id to used.
	 *
	 * @param id
	 */
	public void set(long idToSet) {
		if ((ids == null) || (ids.length == 0)) {
			ids = new long[10];
		}
		// find block that contains idToSet
		int blockIndex;
		// Largest id of the block before the block that contains idToSet
		long maxPreviousId = -1;
		for (blockIndex = 0; (blockIndex < ids.length) && (ids[blockIndex] != 0); blockIndex++) {
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
			} else {
				if (blockIndex > 0) {
					ids[blockIndex - 1]++;
				} else {
					ids[0] = 1;
				}
			}
		} else if (ids[blockIndex] > 0) {
			// the id was already set
			return;
		} else if (ids[blockIndex] == -1) {
			// the only unused id in this block is set
			if (blockIndex == 0) {
				// [-1,x,...] -> [x+1,...]
				System.arraycopy(ids, 1, ids, 0, ids.length - 1);
				ids[0]++;
				ids[ids.length - 1] = 0;
			} else {
				// [..,-w,+x,1,+y,-z,...] -> [...,-w,+x+y+1,-z,...]
				int lastUsedBlockIndex = getNumberOfUsedBlocks() - 1;
				ids[blockIndex - 1] += ids[blockIndex + 1] + 1;
				System.arraycopy(ids, blockIndex + 2, ids, blockIndex, ids.length - blockIndex - 2);
				// Remove leftover data that wasn't overwritten
				ids[lastUsedBlockIndex] = 0;
				ids[lastUsedBlockIndex - 1] = 0;
			}
		} else if ((maxPreviousId + 1) == idToSet) {
			// the first id of an unused block is set
			if (blockIndex == 0) {
				// [-x,...]->[+1,-x+1,...]
				shiftArrayToRight(0, 1);
				ids[0] = 1;
				ids[1]++;
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
			shiftArrayToRight(blockIndex, 2);
			ids[blockIndex] = -x1;
			ids[blockIndex + 1] = 1;
			ids[blockIndex + 2] = -x2;
		}
	}

	public boolean isUsed(long id) {
		if ((ids == null) || (id < 0)) {
			return false;
		}
		// Refers to the last id that the RLE list currently refers to
		long idCounter = -1;
		for (int i = 0; i < ids.length; i++) {
			if (ids[i] == 0) {
				// End of RLE
				return false;
			}
			// Negative value implies the last values aren't used
			boolean idsUsed = ids[i] > 0;
			idCounter += Math.abs(ids[i]);
			if (idCounter >= id) {
				// Found the section where the desired id is included
				return idsUsed;
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
		for (int i = 0; i < ids.length; i++) {
			if (ids[i] == 0) {
				break;
			}
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
		for (int i = 0; i < ids.length; i++) {
			if (ids[i] == 0) {
				break;
			}
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

	public long usedIdsCount() {
		if (ids == null) {
			return 0;
		}
		long count = 0;
		for (int i = 0; i < ids.length; i++) {
			if (ids[i] > 0) {
				count += ids[i];
			}
		}
		return count;
	}

	public void defrag() {
		long maxId = 0;
		for (long idCount : ids) {
			if (idCount > 0) {
				maxId += idCount;
			}
		}
		ids = new long[10];
		ids[0] = maxId;
	}

	public long[] getData() {
		int dataLength = 0;
		for (; (dataLength < ids.length) && (ids[dataLength] != 0); dataLength++) {
			;
		}
		long[] data = new long[dataLength];
		System.arraycopy(ids, 0, data, 0, dataLength);
		return data;
	}

	public void clear() {
		ids = null;
	}

	public boolean isEmpty() {
		return (ids == null) || (ids[0] == 0);
	}

	@Override
	public String toString() {
		return ids == null ? "[]" : Arrays.toString(ids);
	}

	private void extendIdsArray(int minLength) {
		long[] newIds = new long[minLength + 10];
		System.arraycopy(ids, 0, newIds, 0, ids.length);
		ids = newIds;
	}

}
