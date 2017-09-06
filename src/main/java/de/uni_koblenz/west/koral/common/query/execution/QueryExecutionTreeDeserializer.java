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
package de.uni_koblenz.west.koral.common.query.execution;

import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.query.TriplePattern;
import de.uni_koblenz.west.koral.common.query.TriplePatternType;
import de.uni_koblenz.west.koral.common.query.execution.operators.DefaultQueryOperatorTaskFactory;
import de.uni_koblenz.west.koral.common.query.execution.operators.base_impl.QueryBaseOperatorTaskFactory;
import de.uni_koblenz.west.koral.slave.triple_store.TripleStoreAccessor;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;

/**
 * Deserializes the query execution tree received by the slaves.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class QueryExecutionTreeDeserializer {

  private QueryOperatorTaskFactoryBase taskFactory;

  private final TripleStoreAccessor tripleStore;

  private final int numberOfSlaves;

  private final int cacheSize;

  private final File cacheDirectory;

  private final MapDBStorageOptions storageType;

  private final boolean useTransactions;

  private final boolean writeAsynchronously;

  private final MapDBCacheOptions cacheType;

  public QueryExecutionTreeDeserializer(TripleStoreAccessor tripleStore, int numberOfSlaves,
          int cacheSize, File cacheDirectory, MapDBStorageOptions storageType,
          boolean useTransactions, boolean writeAsynchronously, MapDBCacheOptions cacheType) {
    this.tripleStore = tripleStore;
    this.numberOfSlaves = numberOfSlaves;
    this.cacheSize = cacheSize;
    this.cacheDirectory = cacheDirectory;
    this.cacheType = cacheType;
    this.storageType = storageType;
    this.useTransactions = useTransactions;
    this.writeAsynchronously = writeAsynchronously;
  }

  public QueryOperatorTask deserialize(byte[] serializedQET) {
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(serializedQET));) {
      return deserialize(input);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public QueryOperatorTask deserialize(DataInputStream input) throws IOException {
    boolean useBaseImplementation = input.readBoolean();
    long coordinatorId = input.readLong();
    taskFactory = useBaseImplementation
            ? new QueryBaseOperatorTaskFactory(coordinatorId, numberOfSlaves, cacheSize,
                    cacheDirectory)
            : new DefaultQueryOperatorTaskFactory(coordinatorId, numberOfSlaves, cacheSize,
                    cacheDirectory);
    return deserializeQueryOperator(input);
  }

  private QueryOperatorTask deserializeQueryOperator(DataInputStream input) throws IOException {
    switch (QueryOperatorType.valueOf(input.readInt())) {
      case PROJECTION:
        return deserializeProjection(input);
      case TRIPLE_PATTERN_JOIN:
        return deserializeTriplePatternJoin(input);
      case TRIPLE_PATTERN_MATCH:
        return deserializeTriplePatternMatch(input);
      default:
        throw new RuntimeException("Unkonw query operator.");
    }
  }

  private QueryOperatorTask deserializeProjection(DataInputStream input) throws IOException {
    QueryOperatorTask child = deserializeQueryOperator(input);
    long taskId = input.readLong();
    int emittedMappingsPerRound = input.readInt();
    long estimatedTaskLoad = input.readLong();
    int numberOfResultVars = input.readInt();
    long[] resultVars = new long[numberOfResultVars];
    for (int i = 0; i < resultVars.length; i++) {
      resultVars[i] = input.readLong();
    }

    QueryOperatorBase result = (QueryOperatorBase) taskFactory.createProjection(taskId,
            emittedMappingsPerRound, resultVars, child);
    result.setEstimatedWorkLoad(estimatedTaskLoad);
    ((QueryOperatorBase) child).setParentTask(result);
    return result;
  }

  private QueryOperatorTask deserializeTriplePatternJoin(DataInputStream input) throws IOException {
    QueryOperatorTask leftChild = deserializeQueryOperator(input);
    QueryOperatorTask rightChild = deserializeQueryOperator(input);
    long taskId = input.readLong();
    int emittedMappingsPerRound = input.readInt();
    long estimatedTaskLoad = input.readLong();

    QueryOperatorBase result = (QueryOperatorBase) taskFactory.createTriplePatternJoin(taskId,
            emittedMappingsPerRound, leftChild, rightChild, storageType, useTransactions,
            writeAsynchronously, cacheType);
    result.setEstimatedWorkLoad(estimatedTaskLoad);
    ((QueryOperatorBase) leftChild).setParentTask(result);
    ((QueryOperatorBase) rightChild).setParentTask(result);
    return result;
  }

  private QueryOperatorTask deserializeTriplePatternMatch(DataInputStream input)
          throws IOException {
    long taskId = input.readLong();
    int emittedMappingsPerRound = input.readInt();
    long estimatedTaskLoad = input.readLong();

    int patternType = input.readInt();
    long subject = input.readLong();
    long property = input.readLong();
    long object = input.readLong();
    TriplePattern pattern = new TriplePattern(TriplePatternType.valueOf(patternType), subject,
            property, object);

    QueryOperatorBase result = (QueryOperatorBase) taskFactory.createTriplePatternMatch(taskId,
            emittedMappingsPerRound, pattern, tripleStore);
    result.setEstimatedWorkLoad(estimatedTaskLoad);
    return result;
  }

}
