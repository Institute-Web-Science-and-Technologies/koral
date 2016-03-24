package de.uni_koblenz.west.cidre.common.executor;

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
