package de.uni_koblenz.west.cidre.common.query.execution;

/**
 * Supertype of all query operations.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface QueryOperatorTask {

	public long[] getResultVariables();

	/**
	 * @return -1 iff no join var exists
	 */
	public long getFirstJoinVar();

}
