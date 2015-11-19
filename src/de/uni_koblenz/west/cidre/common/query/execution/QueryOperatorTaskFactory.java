package de.uni_koblenz.west.cidre.common.query.execution;

import org.apache.jena.graph.Triple;

/**
 * Provides methods to create the query execution tree.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class QueryOperatorTaskFactory {

	public static QueryOperatorTask createTriplePatternMatch(Triple triple) {
		// TODO Auto-generated method stub
		return new DummyTask();
	}

	public static QueryOperatorTask createTriplePatternJoin(QueryOperatorTask left,
			QueryOperatorTask right) {
		// TODO Auto-generated method stub
		return new DummyTask();
	}

	public static QueryOperatorTask createProjection(QueryOperatorTask subTask) {
		// TODO Auto-generated method stub
		return new DummyTask();
	}

}

/**
 * TODO Delete this. Only used for testing implementation until QueryTasks are
 * implemented.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
class DummyTask implements QueryOperatorTask {

	@Override
	public String[] getResultVariables() {
		// TODO Auto-generated method stub
		return new String[0];
	}

	@Override
	public String getFirstJoinVar() {
		// TODO Auto-generated method stub
		return null;
	}

}
