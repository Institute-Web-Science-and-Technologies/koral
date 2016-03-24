package de.uni_koblenz.west.cidre.common.executor;

import de.uni_koblenz.west.cidre.common.executor.messagePassing.MessageSenderBuffer;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;

import java.io.Closeable;
import java.util.Set;
import java.util.logging.Logger;

/**
 * This interface declares all methods required to execute a task in the
 * execution framework of CIDRE.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface WorkerTask extends Closeable {

  public void setUp(MessageSenderBuffer messageSender, MappingRecycleCache recycleCache,
          Logger logger);

  /**
   * <p>
   * The id consists of:
   * <ol>
   * <li>2 bytes computer id</li>
   * <li>4 bytes query id</li>
   * <li>2 bytes query node id</li>
   * </ol>
   * </p>
   * 
   * <p>
   * The ids of two {@link WorkerTask}s should only be equal, if
   * task1.equals(task2)==true
   * </p>
   * 
   * @return
   */
  public long getID();

  /**
   * @return id of the query coordinator node of the query execution tree
   */
  public long getCoordinatorID();

  public long getEstimatedTaskLoad();

  public long getCurrentTaskLoad();

  public WorkerTask getParentTask();

  public Set<WorkerTask> getPrecedingTasks();

  /**
   * Starts to emit results on the next {@link #execute()} call.
   */
  public void start();

  public boolean hasInput();

  public boolean hasToPerformFinalSteps();

  public void enqueueMessage(long sender, byte[] message, int firstIndex, int lengthOfMessage);

  /**
   * Results may only be emitted, it {@link #start()} was called.
   */
  public void execute();

  /**
   * @return true, if task has successfully finished or if it was aborted
   */
  public boolean isInFinalState();

  @Override
  public void close();

  @Override
  public String toString();

}
