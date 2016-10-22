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

  private static final long MAX_NUMBER_OF_IDS = (2l * Integer.MAX_VALUE) + 1;
  /**
   * even indices represent free ids<br>
   * odd indices represent used ids
   */
  private long[] ids;

  public int getNextId() {
    if ((ids == null) || (ids.length == 0)) {
      ids = new long[] { 0, 0 };
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
      if (ids[1] == ReusableIDGenerator.MAX_NUMBER_OF_IDS) {
        throw new RuntimeException("There are no more free ids in the int range.");
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
    if ((ids == null) || (ids.length == 0)) {
      return;
    }
    long longQueryId = queryId & 0x00_00_00_00_ff_ff_ff_ffl;
    // find index to whom's block the queryId belong
    long firstIdOfBlock = 0;
    int indexOfBlock;
    for (indexOfBlock = 0; (indexOfBlock < ids.length)
            && (((firstIdOfBlock + ids[indexOfBlock]) - 1) < longQueryId); indexOfBlock++) {
      firstIdOfBlock += ids[indexOfBlock];
    }
    if (indexOfBlock > ids.length) {
      // queryId is larger than the largest queryId stored in the id array
      return;
    } else if ((indexOfBlock % 2) == 0) {
      // queryId resides in an free block
      return;
    } else {
      // queryId is marked as used, yet
      assert ids[indexOfBlock] > 0;
      if (ids[indexOfBlock] == 1) {
        // the query id is the last in a used block
        long[] newIds = new long[ids.length - 2];
        if (indexOfBlock == (ids.length - 1)) {
          // [...,f1,u,f2,1] has to be changed to [...,f1,u]
          System.arraycopy(ids, 0, newIds, 0, newIds.length);
        } else {
          // [...,f1,1,f2,u,...] has to be changed to
          // [...,f1+1+f2,u,...]
          for (int oldI = 0, newI = 0; oldI < ids.length; oldI++, newI++) {
            if (oldI == indexOfBlock) {
              newIds[newI - 1] += 1 + ids[oldI + 1];
              newI--;
              oldI++;
            } else {
              newIds[newI] = ids[oldI];
            }
          }
        }
        ids = newIds;
      } else if (longQueryId == firstIdOfBlock) {
        // the query id is the first of a used block>1
        // [...,f,u,...] has to be changed to [...,f+1,u-1,...]
        ids[indexOfBlock - 1]++;
        ids[indexOfBlock]--;
      } else if (longQueryId == ((firstIdOfBlock + ids[indexOfBlock]) - 1)) {
        // the query id is the last of a used block>1
        // [...,f,u] has to be changed to [...,f,u-1]
        ids[indexOfBlock]--;
        if (indexOfBlock < (ids.length - 1)) {
          // [...,f1,u,f2,...] has to be changed to
          // [...,f1,u-1,f2+1,...]
          ids[indexOfBlock + 1]++;
        }
      } else {
        // the query id is in the middle of a used block
        // [...,f1,u,f2,...] has to be changed to
        // [...,f1,un1,1,un2,f2,...]
        // with u=un1+1+un2
        long[] newIds = new long[ids.length + 2];
        for (int oldI = 0, newI = 0; oldI < ids.length; oldI++, newI++) {
          if (oldI == indexOfBlock) {
            newIds[newI] = longQueryId - firstIdOfBlock;
            newIds[newI + 1] = 1;
            newIds[newI + 2] = (firstIdOfBlock + ids[oldI]) - 1 - longQueryId;
            newI += 2;
          } else {
            newIds[newI] = ids[oldI];
          }
        }
        ids = newIds;
      }
    }
  }

  @Override
  public String toString() {
    return ids == null ? "[]" : Arrays.toString(ids);
  }

}
