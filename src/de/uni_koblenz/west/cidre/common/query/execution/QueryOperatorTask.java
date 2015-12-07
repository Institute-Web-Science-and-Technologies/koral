package de.uni_koblenz.west.cidre.common.query.execution;

import java.io.DataOutputStream;
import java.io.IOException;

import de.uni_koblenz.west.cidre.common.executor.WorkerTask;

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

	public byte[] serialize(boolean useBaseImplementation);

	public void serialize(DataOutputStream output,
			boolean useBaseImplementation) throws IOException;

}
