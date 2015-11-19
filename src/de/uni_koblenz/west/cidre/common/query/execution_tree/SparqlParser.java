package de.uni_koblenz.west.cidre.common.query.execution_tree;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.algebra.op.OpAssign;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpConditional;
import org.apache.jena.sparql.algebra.op.OpDatasetNames;
import org.apache.jena.sparql.algebra.op.OpDiff;
import org.apache.jena.sparql.algebra.op.OpDisjunction;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.algebra.op.OpExt;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpGraph;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpLabel;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpList;
import org.apache.jena.sparql.algebra.op.OpMinus;
import org.apache.jena.sparql.algebra.op.OpNull;
import org.apache.jena.sparql.algebra.op.OpOrder;
import org.apache.jena.sparql.algebra.op.OpPath;
import org.apache.jena.sparql.algebra.op.OpProcedure;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.algebra.op.OpPropFunc;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpQuadBlock;
import org.apache.jena.sparql.algebra.op.OpQuadPattern;
import org.apache.jena.sparql.algebra.op.OpReduced;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.op.OpTopN;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.algebra.op.OpUnion;

import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorTaskFactory;

/**
 * Checks whether the query only consists of the supported operations and
 * transforms it into the CIDRE-specific query execution tree.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class SparqlParser implements OpVisitor {

	private QueryExecutionTreeType treeType;

	private final Deque<QueryOperatorTask> stack;

	public SparqlParser() {
		stack = new ArrayDeque<>();
	}

	/*
	 * http://www.w3.org/TR/sparql11-query/#sparqlDefinition
	 * https://jena.apache.org/documentation/query/algebra.html
	 * https://jena.apache.org/documentation/notes/sse.html
	 */

	public QueryOperatorTask parse(String queryString,
			QueryExecutionTreeType treeType) {
		this.treeType = treeType;
		Query queryObject = QueryFactory.create(queryString);
		if (!queryObject.isSelectType()) {
			throw new UnsupportedOperationException(
					"Currently, CIDRE only supports SELECT queries, but your query is\n"
							+ queryObject.serialize());
		}
		Op op = Algebra.compile(queryObject);
		op.visit(this);
		assert stack.size() == 1;
		return stack.pop();
	}

	@Override
	public void visit(OpBGP opBGP) {
		Iterator<Triple> tripleIter = opBGP.getPattern().getList().iterator();
		int numberOfTriplePattern = 0;
		while (tripleIter.hasNext()) {
			Triple triple = tripleIter.next();
			visit(triple);
			numberOfTriplePattern++;
			switch (treeType) {
			case LEFT_LINEAR:
				if (numberOfTriplePattern > 1) {
					QueryOperatorTask left = stack.pop();
					QueryOperatorTask right = stack.pop();
					QueryOperatorTask join = QueryOperatorTaskFactory
							.createTriplePatternJoin(left, right);
					stack.push(join);
				}
				break;
			case RIGHT_LINEAR:
				if (!tripleIter.hasNext()) {
					for (int i = 1; i < numberOfTriplePattern; i++) {
						QueryOperatorTask right = stack.pop();
						QueryOperatorTask left = stack.pop();
						QueryOperatorTask join = QueryOperatorTaskFactory
								.createTriplePatternJoin(left, right);
						stack.push(join);
					}
				}
				break;
			case BUSHY:
				if (!tripleIter.hasNext()) {
					createBushyTree(numberOfTriplePattern);
				}
				break;
			}
		}
	}

	private void createBushyTree(int numberOfTriplePattern) {
		Queue<QueryOperatorTask> workingQueue = new LinkedList<>();
		for (int i = 0; i < numberOfTriplePattern; i++) {
			((LinkedList<QueryOperatorTask>) workingQueue).addFirst(stack.pop());
		}
		Queue<QueryOperatorTask> nextWorkingQueue = new LinkedList<>();
		while (!workingQueue.isEmpty()) {
			QueryOperatorTask leftChild = workingQueue.poll();
			if (workingQueue.isEmpty()) {
				nextWorkingQueue.offer(leftChild);
			} else {
				QueryOperatorTask rightChild = workingQueue.poll();
				QueryOperatorTask join = QueryOperatorTaskFactory
						.createTriplePatternJoin(leftChild, rightChild);
				nextWorkingQueue.offer(join);
			}
			if (workingQueue.isEmpty() && nextWorkingQueue.size() > 1) {
				workingQueue = nextWorkingQueue;
				nextWorkingQueue = new LinkedList<>();
			}
		}
		stack.push(nextWorkingQueue.poll());
	}

	public void visit(Triple triple) {
		QueryOperatorTask task = QueryOperatorTaskFactory.createTriplePatternMatch(triple);
		stack.push(task);
	}

	@Override
	public void visit(OpQuadPattern quadPattern) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support quad patterns. Cause:\n"
						+ quadPattern.toString());
	}

	@Override
	public void visit(OpQuadBlock quadBlock) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support quad blocks. Cause:\n"
						+ quadBlock.toString());
	}

	@Override
	public void visit(OpTriple opTriple) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support triple. Cause:\n"
						+ opTriple.toString());
	}

	@Override
	public void visit(OpQuad opQuad) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support quad. Cause:\n"
						+ opQuad.toString());
	}

	@Override
	public void visit(OpPath opPath) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support path. Cause:\n"
						+ opPath.toString());
	}

	@Override
	public void visit(OpTable opTable) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support table. Cause:\n"
						+ opTable.toString());
	}

	@Override
	public void visit(OpNull opNull) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support null. Cause:\n"
						+ opNull.toString());
	}

	@Override
	public void visit(OpProcedure opProc) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support proc. Cause:\n"
						+ opProc.toString());
	}

	@Override
	public void visit(OpPropFunc opPropFunc) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support prop func. Cause:\n"
						+ opPropFunc.toString());
	}

	@Override
	public void visit(OpFilter opFilter) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support filter. Cause:\n"
						+ opFilter.toString());
	}

	@Override
	public void visit(OpGraph opGraph) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support graph. Cause:\n"
						+ opGraph.toString());
	}

	@Override
	public void visit(OpService opService) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support service. Cause:\n"
						+ opService.toString());
	}

	@Override
	public void visit(OpDatasetNames dsNames) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support dsNames. Cause:\n"
						+ dsNames.toString());
	}

	@Override
	public void visit(OpLabel opLabel) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support label. Cause:\n"
						+ opLabel.toString());
	}

	@Override
	public void visit(OpAssign opAssign) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support assign. Cause:\n"
						+ opAssign.toString());
	}

	@Override
	public void visit(OpExtend opExtend) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support extend. Cause:\n"
						+ opExtend.toString());
	}

	@Override
	public void visit(OpJoin opJoin) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support join. Cause:\n"
						+ opJoin.toString());
	}

	@Override
	public void visit(OpLeftJoin opLeftJoin) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support left join. Cause:\n"
						+ opLeftJoin.toString());
	}

	@Override
	public void visit(OpUnion opUnion) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support union. Cause:\n"
						+ opUnion.toString());
	}

	@Override
	public void visit(OpDiff opDiff) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support diff. Cause:\n"
						+ opDiff.toString());
	}

	@Override
	public void visit(OpMinus opMinus) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support minus. Cause:\n"
						+ opMinus.toString());
	}

	@Override
	public void visit(OpConditional opCondition) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support conditional. Cause:\n"
						+ opCondition.toString());
	}

	@Override
	public void visit(OpSequence opSequence) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support sequence. Cause:\n"
						+ opSequence.toString());
	}

	@Override
	public void visit(OpDisjunction opDisjunction) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support disjunction. Cause:\n"
						+ opDisjunction.toString());
	}

	@Override
	public void visit(OpExt opExt) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support ext. Cause:\n"
						+ opExt.toString());
	}

	@Override
	public void visit(OpList opList) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support list. Cause:\n"
						+ opList.toString());
	}

	@Override
	public void visit(OpOrder opOrder) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support order. Cause:\n"
						+ opOrder.toString());
	}

	@Override
	public void visit(OpProject opProject) {
		opProject.getSubOp().visit(this);
		QueryOperatorTask subTask = stack.pop();
		QueryOperatorTask projection = QueryOperatorTaskFactory.createProjection(subTask);
		stack.push(projection);
	}

	@Override
	public void visit(OpReduced opReduced) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support reduced. Cause:\n"
						+ opReduced.toString());
	}

	@Override
	public void visit(OpDistinct opDistinct) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support distinct. Cause:\n"
						+ opDistinct.toString());
	}

	@Override
	public void visit(OpSlice opSlice) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support slice. Cause:\n"
						+ opSlice.toString());
	}

	@Override
	public void visit(OpGroup opGroup) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support group. Cause:\n"
						+ opGroup.toString());
	}

	@Override
	public void visit(OpTopN opTop) {
		throw new UnsupportedOperationException(
				"Currently, CIDRE does not support top. Cause:\n"
						+ opTop.toString());
	}

}
