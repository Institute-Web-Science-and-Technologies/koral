package de.uni_koblenz.west.cidre.master.tasks;

import de.uni_koblenz.west.cidre.common.messages.MessageListener;

/**
 * Processes notifications from slaves whether they have finished or failed to
 * load their graph chunk.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class GraphLoaderListener implements MessageListener {

  private final GraphLoaderTask task;

  private final int slaveId;

  public GraphLoaderListener(GraphLoaderTask task, int slaveId) {
    this.task = task;
    this.slaveId = slaveId;
  }

  @Override
  public void processMessage(byte[][] message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void processMessage(byte[] message) {
    task.processSlaveResponse(message);
  }

  @Override
  public int getSlaveID() {
    return slaveId;
  }

  @Override
  public void close() {
  }

}
