package de.uni_koblenz.west.cidre.common.query.execution.operators;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.OpProject;

import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.cidre.common.query.parser.VariableDictionary;

/**
 * Provides methods to create the query execution tree.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class QueryOperatorTaskFactory {

	public static QueryOperatorTask createTriplePatternMatch(Triple triple,
			VariableDictionary dictionary) {
		// TODO Auto-generated method stub
		return new DummyTask();
	}

	public static QueryOperatorTask createTriplePatternJoin(
			QueryOperatorTask left, QueryOperatorTask right) {
		// TODO Auto-generated method stub
		return new DummyTask();
	}

	public static QueryOperatorTask createProjection(OpProject opProject,
			QueryOperatorTask subTask, VariableDictionary dictionary) {
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
	public long[] getResultVariables() {
		// TODO Auto-generated method stub
		return new long[0];
	}

	@Override
	public long getFirstJoinVar() {
		// TODO Auto-generated method stub
		return 0l;
	}

}
