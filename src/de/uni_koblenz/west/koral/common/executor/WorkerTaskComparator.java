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
package de.uni_koblenz.west.koral.common.executor;

import java.util.Comparator;

/**
 * This {@link Comparator} is used to sort the {@link WorkerTask}s of one
 * {@link WorkerThread} according to its workload during the rescheduling step.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class WorkerTaskComparator implements Comparator<WorkerTask> {

  private final boolean useEstimation;

  public WorkerTaskComparator(boolean useEstimation) {
    this.useEstimation = useEstimation;
  }

  @Override
  public int compare(WorkerTask o1, WorkerTask o2) {
    long diff = useEstimation ? o1.getEstimatedTaskLoad() - o2.getEstimatedTaskLoad()
            : (o1.getCurrentTaskLoad() - o2.getCurrentTaskLoad());
    if (diff < 0) {
      return -1;
    } else if (diff > 0) {
      return 1;
    } else {
      diff = o1.getID() - o2.getID();
      return diff == 0 ? 0 : diff < 0 ? -1 : 1;
    }
  }

}
