package de.uni_koblenz.west.cidre.common.query.execution;

/**
 * Supertype of all query operations.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface QueryOperatorTask {

	public String[] getResultVariables();

	public String getFirstJoinVar();

}
