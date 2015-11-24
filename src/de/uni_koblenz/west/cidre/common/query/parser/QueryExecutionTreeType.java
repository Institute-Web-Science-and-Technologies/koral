package de.uni_koblenz.west.cidre.common.query.parser;

/**
 * Defines the order in which the triple patterns of a BGP should be joined.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public enum QueryExecutionTreeType {

	LEFT_LINEAR, RIGHT_LINEAR, BUSHY;

}
