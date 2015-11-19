package de.uni_koblenz.west.cidre.common.query.execution;

/**
 * Defines the different states of a {@link QueryOperatorBase} instance.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public enum QueryTaskState {

	CREATED, STARTED, WAITING_FOR_OTHERS_TO_FINISH, FINISHED, ABORTED;

}
