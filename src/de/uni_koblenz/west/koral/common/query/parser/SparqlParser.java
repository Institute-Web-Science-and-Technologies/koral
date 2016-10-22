/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License
 * along with Koral.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
package de.uni_koblenz.west.koral.common.query.parser;

import org.apache.jena.graph.Node;
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
import org.apache.jena.sparql.core.Var;

import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.query.TriplePattern;
import de.uni_koblenz.west.koral.common.query.TriplePatternType;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorBase;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorTaskFactoryBase;
import de.uni_koblenz.west.koral.common.query.execution.operators.DefaultQueryOperatorTaskFactory;
import de.uni_koblenz.west.koral.common.query.execution.operators.base_impl.QueryBaseOperatorTaskFactory;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.koral.slave.triple_store.TripleStoreAccessor;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Checks whether the query only consists of the supported operations and
 * transforms it into the Koral-specific query execution tree.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class SparqlParser implements OpVisitor {

  private QueryExecutionTreeType treeType;

  private QueryOperatorTaskFactoryBase taskFactory;

  private final TripleStoreAccessor tripleStore;

  private final DictionaryEncoder dictionary;

  private VariableDictionary varDictionary;

  private final Deque<QueryOperatorTask> stack;

  private final short slaveId;

  private final int queryId;

  private final int emittedMappingsPerRound;

  private final MapDBStorageOptions storageType;

  private final boolean useTransactions;

  private final boolean writeAsynchronously;

  private final MapDBCacheOptions cacheType;

  private final GraphStatistics statistics;

  public SparqlParser(DictionaryEncoder dictionary, GraphStatistics statistics,
          TripleStoreAccessor tripleStore, short slaveId, int queryId, long coordinatorId,
          int numberOfSlaves, int cacheSize, File cacheDirectory, int emittedMappingsPerRound,
          MapDBStorageOptions storageType, boolean useTransactions, boolean writeAsynchronously,
          MapDBCacheOptions cacheType) {
    this(dictionary, statistics, tripleStore, slaveId, queryId, coordinatorId, numberOfSlaves,
            cacheSize, cacheDirectory, emittedMappingsPerRound, storageType, useTransactions,
            writeAsynchronously, cacheType, false);
  }

  public SparqlParser(DictionaryEncoder dictionary, GraphStatistics statistics,
          TripleStoreAccessor tripleStore, short slaveId, int queryId, long coordinatorId,
          int numberOfSlaves, int cacheSize, File cacheDirectory, int emittedMappingsPerRound,
          MapDBStorageOptions storageType, boolean useTransactions, boolean writeAsynchronously,
          MapDBCacheOptions cacheType, boolean useBaseImplementation) {
    this.dictionary = dictionary;
    this.statistics = statistics;
    this.tripleStore = tripleStore;
    stack = new ArrayDeque<>();
    this.slaveId = slaveId;
    this.queryId = queryId;
    if (useBaseImplementation) {
      taskFactory = new QueryBaseOperatorTaskFactory(coordinatorId, numberOfSlaves, cacheSize,
              cacheDirectory);
    } else {
      taskFactory = new DefaultQueryOperatorTaskFactory(coordinatorId, numberOfSlaves, cacheSize,
              cacheDirectory);
    }
    this.emittedMappingsPerRound = emittedMappingsPerRound;
    this.cacheType = cacheType;
    this.storageType = storageType;
    this.useTransactions = useTransactions;
    this.writeAsynchronously = writeAsynchronously;
  }

  public void setUseBaseImplementation(boolean useBaseOperators) {
    if (useBaseOperators && !(taskFactory instanceof QueryBaseOperatorTaskFactory)) {
      taskFactory = new QueryBaseOperatorTaskFactory(taskFactory);
    } else if (!useBaseOperators && !(taskFactory instanceof DefaultQueryOperatorTaskFactory)) {
      taskFactory = new DefaultQueryOperatorTaskFactory(taskFactory);
    }
  }

  public boolean isBaseImplementationUsed() {
    return taskFactory instanceof QueryBaseOperatorTaskFactory;
  }

  /*
   * http://www.w3.org/TR/sparql11-query/#sparqlDefinition
   * https://jena.apache.org/documentation/query/algebra.html
   * https://jena.apache.org/documentation/notes/sse.html
   */

  public QueryOperatorTask parse(String queryString, QueryExecutionTreeType treeType,
          VariableDictionary dictionary) {
    this.treeType = treeType;
    varDictionary = dictionary;
    Query queryObject = QueryFactory.create(queryString);
    if (!queryObject.isSelectType()) {
      throw new UnsupportedOperationException(
              "Currently, Koral only supports SELECT queries, but your query is\n"
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
            QueryOperatorTask right = stack.pop();
            QueryOperatorTask left = stack.pop();
            QueryOperatorTask join = createTriplePatternJoin(left, right);
            stack.push(join);
          }
          break;
        case RIGHT_LINEAR:
          if (!tripleIter.hasNext()) {
            for (int i = 1; i < numberOfTriplePattern; i++) {
              QueryOperatorTask right = stack.pop();
              QueryOperatorTask left = stack.pop();
              QueryOperatorTask join = createTriplePatternJoin(left, right);
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
        QueryOperatorTask join = createTriplePatternJoin(leftChild, rightChild);
        nextWorkingQueue.offer(join);
      }
      if (workingQueue.isEmpty() && (nextWorkingQueue.size() > 1)) {
        workingQueue = nextWorkingQueue;
        nextWorkingQueue = new LinkedList<>();
      }
    }
    stack.push(nextWorkingQueue.poll());
  }

  private QueryOperatorTask createTriplePatternJoin(QueryOperatorTask left,
          QueryOperatorTask right) {
    QueryOperatorTask join = taskFactory.createTriplePatternJoin(slaveId, queryId,
            emittedMappingsPerRound, left, right, storageType, useTransactions, writeAsynchronously,
            cacheType);
    ((QueryOperatorBase) left).setParentTask(join);
    ((QueryOperatorBase) right).setParentTask(join);
    return join;
  }

  public void visit(Triple triple) {
    TriplePatternType type = TriplePatternType.SPO;
    long subject = 0;
    long property = 0;
    long object = 0;

    if ((varDictionary != null) && (dictionary != null)) {
      Node subjectN = triple.getSubject();
      if (subjectN.isVariable()) {
        subject = varDictionary.encode(subjectN.getName());
        type = TriplePatternType._PO;
      } else {
        subject = dictionary.encode(subjectN, false, statistics);
      }

      Node propertyN = triple.getPredicate();
      if (propertyN.isVariable()) {
        property = varDictionary.encode(propertyN.getName());
        if (type == TriplePatternType.SPO) {
          type = TriplePatternType.S_O;
        } else {
          type = TriplePatternType.__O;
        }
      } else {
        property = dictionary.encode(propertyN, false, statistics);
      }

      Node objectN = triple.getObject();
      if (objectN.isVariable()) {
        object = varDictionary.encode(objectN.getName());
        if (type == TriplePatternType.SPO) {
          type = TriplePatternType.SP_;
        } else if (type == TriplePatternType._PO) {
          type = TriplePatternType._P_;
        } else if (type == TriplePatternType.S_O) {
          type = TriplePatternType.S__;
        } else {
          type = TriplePatternType.___;
        }
      } else {
        object = dictionary.encode(objectN, false, statistics);
      }
    }

    TriplePattern pattern = new TriplePattern(type, subject, property, object);
    QueryOperatorTask task = taskFactory.createTriplePatternMatch(slaveId, queryId,
            emittedMappingsPerRound, pattern, tripleStore);
    stack.push(task);
  }

  @Override
  public void visit(OpQuadPattern quadPattern) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support quad patterns. Cause:\n" + quadPattern.toString());
  }

  @Override
  public void visit(OpQuadBlock quadBlock) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support quad blocks. Cause:\n" + quadBlock.toString());
  }

  @Override
  public void visit(OpTriple opTriple) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support triple. Cause:\n" + opTriple.toString());
  }

  @Override
  public void visit(OpQuad opQuad) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support quad. Cause:\n" + opQuad.toString());
  }

  @Override
  public void visit(OpPath opPath) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support path. Cause:\n" + opPath.toString());
  }

  @Override
  public void visit(OpTable opTable) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support table. Cause:\n" + opTable.toString());
  }

  @Override
  public void visit(OpNull opNull) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support null. Cause:\n" + opNull.toString());
  }

  @Override
  public void visit(OpProcedure opProc) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support proc. Cause:\n" + opProc.toString());
  }

  @Override
  public void visit(OpPropFunc opPropFunc) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support prop func. Cause:\n" + opPropFunc.toString());
  }

  @Override
  public void visit(OpFilter opFilter) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support filter. Cause:\n" + opFilter.toString());
  }

  @Override
  public void visit(OpGraph opGraph) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support graph. Cause:\n" + opGraph.toString());
  }

  @Override
  public void visit(OpService opService) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support service. Cause:\n" + opService.toString());
  }

  @Override
  public void visit(OpDatasetNames dsNames) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support dsNames. Cause:\n" + dsNames.toString());
  }

  @Override
  public void visit(OpLabel opLabel) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support label. Cause:\n" + opLabel.toString());
  }

  @Override
  public void visit(OpAssign opAssign) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support assign. Cause:\n" + opAssign.toString());
  }

  @Override
  public void visit(OpExtend opExtend) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support extend. Cause:\n" + opExtend.toString());
  }

  @Override
  public void visit(OpJoin opJoin) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support join. Cause:\n" + opJoin.toString());
  }

  @Override
  public void visit(OpLeftJoin opLeftJoin) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support left join. Cause:\n" + opLeftJoin.toString());
  }

  @Override
  public void visit(OpUnion opUnion) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support union. Cause:\n" + opUnion.toString());
  }

  @Override
  public void visit(OpDiff opDiff) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support diff. Cause:\n" + opDiff.toString());
  }

  @Override
  public void visit(OpMinus opMinus) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support minus. Cause:\n" + opMinus.toString());
  }

  @Override
  public void visit(OpConditional opCondition) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support conditional. Cause:\n" + opCondition.toString());
  }

  @Override
  public void visit(OpSequence opSequence) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support sequence. Cause:\n" + opSequence.toString());
  }

  @Override
  public void visit(OpDisjunction opDisjunction) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support disjunction. Cause:\n" + opDisjunction.toString());
  }

  @Override
  public void visit(OpExt opExt) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support ext. Cause:\n" + opExt.toString());
  }

  @Override
  public void visit(OpList opList) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support list. Cause:\n" + opList.toString());
  }

  @Override
  public void visit(OpOrder opOrder) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support order. Cause:\n" + opOrder.toString());
  }

  @Override
  public void visit(OpProject opProject) {
    opProject.getSubOp().visit(this);
    List<Var> vars = opProject.getVars();
    long[] resultVars = new long[vars.size()];
    int index = 0;
    if (varDictionary != null) {
      for (Var var : vars) {
        resultVars[index++] = varDictionary.encode(var.getName());
      }
    }

    QueryOperatorTask subTask = stack.pop();
    QueryOperatorTask projection = taskFactory.createProjection(slaveId, queryId,
            emittedMappingsPerRound, resultVars, subTask);
    ((QueryOperatorBase) subTask).setParentTask(projection);
    long[] varsOfChild = subTask.getResultVariables();
    long[] resultVarsOfProjection = projection.getResultVariables();
    checkIfAllProjectedVariablesAreBound(resultVarsOfProjection, varsOfChild);
    stack.push(projection);
  }

  private boolean checkIfAllProjectedVariablesAreBound(long[] resultVarsOfProjection,
          long[] varsOfChild) {
    for (long selectedVar : resultVarsOfProjection) {
      boolean isUnbound = true;
      for (long childVar : varsOfChild) {
        if (selectedVar == childVar) {
          isUnbound = false;
          break;
        }
      }
      if (isUnbound) {
        throw new RuntimeException("The variable ?" + varDictionary.decode(selectedVar)
                + " of the select operation is unbound.");
      }
    }
    return true;
  }

  @Override
  public void visit(OpReduced opReduced) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support reduced. Cause:\n" + opReduced.toString());
  }

  @Override
  public void visit(OpDistinct opDistinct) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support distinct. Cause:\n" + opDistinct.toString());
  }

  @Override
  public void visit(OpSlice opSlice) {
    opSlice.getSubOp().visit(this);
    long offset = opSlice.getStart();
    long length = opSlice.getLength();

    QueryOperatorTask subTask = stack.pop();
    QueryOperatorTask slice = taskFactory.createSlice(slaveId, queryId, emittedMappingsPerRound,
            subTask, offset, length);
    ((QueryOperatorBase) subTask).setParentTask(slice);
    stack.push(slice);
  }

  @Override
  public void visit(OpGroup opGroup) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support group. Cause:\n" + opGroup.toString());
  }

  @Override
  public void visit(OpTopN opTop) {
    throw new UnsupportedOperationException(
            "Currently, Koral does not support top. Cause:\n" + opTop.toString());
  }

}
