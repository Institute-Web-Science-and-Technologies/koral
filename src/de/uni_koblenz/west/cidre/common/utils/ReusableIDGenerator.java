package de.uni_koblenz.west.cidre.common.utils;

import java.util.Arrays;

/**
 * Returns the first unused int id.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class ReusableIDGenerator {

	private static final long MAX_NUMBER_OF_IDS = 2l * Integer.MAX_VALUE + 1;
	/**
	 * even indices represent free ids<br>
	 * odd indices represent used ids
	 */
	private long[] ids;

	public int getNextId() {
		if (ids == null) {
			ids = new long[] { MAX_NUMBER_OF_IDS, 0 };
		}
		int firstFreeID = 0;
		if (ids[0] > 0) {
			// there are free ids starting with 0
			// use id 0
			if (ids[0] > 1) {
				// [f,u,...] has to be changed to [0,1,z-1,u,...]
				long[] newIds = new long[ids.length + 2];
				System.arraycopy(ids, 0, newIds, 2, ids.length);
				ids = newIds;
				ids[0] = 0;
				ids[1] = 1;
				ids[2]--;
			} else {
				// [1,u,...] has to be changed to [0,u+1,...]
				ids[0] = 0;
				ids[1]++;
			}
		} else if (ids.length == 2) {
			if (ids[1] == MAX_NUMBER_OF_IDS) {
				throw new RuntimeException(
						"There are no more free ids in the int range.");
			} else {
				// overflow during cast ensures that after Integer.MAX_VALUE,
				// Integer.MIN_VALUE is returned!
				firstFreeID = (int) ids[1];
				// [0,u] has to be changed to [0,u+1]
				ids[1]++;
			}
		} else if (ids[2] > 0) {
			// overflow during cast ensures that after Integer.MAX_VALUE,
			// Integer.MIN_VALUE is returned!
			firstFreeID = (int) ids[1];
			if (ids[2] > 1) {
				// [0,u,f,...] has to be changed to [0,u+1,f-1,...]
				ids[1]++;
				ids[2]--;
			} else {
				// [0,u1,1,u2,...] has to be changed to [0,u1+1+u2,...]
				long[] newIds = new long[ids.length - 2];
				newIds[1] = ids[1] + 1 + ids[3];
				if (newIds.length > 2) {
					System.arraycopy(ids, 4, newIds, 2, ids.length - 4);
				}
				ids = newIds;
			}
		}
		return firstFreeID;
	}

	public void release(int queryId) {
		// TODO Auto-generated method stub

	}

	@Override
	public String toString() {
		return Arrays.toString(ids);
	}

}
