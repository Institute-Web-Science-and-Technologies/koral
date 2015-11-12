package de.uni_koblenz.west.cidre.common.query.execution;

import org.apache.jena.graph.Triple;

/**
 * Provides methods to create the query execution tree.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class QueryTaskFactory {

	public static QueryTask createTriplePatternMatch(Triple triple) {
		// TODO Auto-generated method stub
		return new DummyTask();
	}

	public static QueryTask createTriplePatternJoin(QueryTask left,
			QueryTask right) {
		// TODO Auto-generated method stub
		return new DummyTask();
	}

	public static QueryTask createProjection(QueryTask subTask) {
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
class DummyTask implements QueryTask {

}
