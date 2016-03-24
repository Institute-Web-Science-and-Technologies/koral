package de.uni_koblenz.west.cidre.common.query.execution;

import de.uni_koblenz.west.cidre.common.executor.WorkerTask;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Supertype of all query operations.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface QueryOperatorTask extends WorkerTask {

  public long[] getResultVariables();

  /**
   * @return -1 iff no join var exists
   */
  public long getFirstJoinVar();

  public byte[] serialize(boolean useBaseImplementation, int slaveId);

  public void serialize(DataOutputStream output, boolean useBaseImplementation, int slaveId)
          throws IOException;

}
