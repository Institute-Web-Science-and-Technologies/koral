/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License along with Koral. If not,
 * see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
package playground;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import org.apache.jena.query.QueryFactory;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.common.query.execution.QueryExecutionTreeDeserializer;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorBase;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.koral.common.query.parser.QueryExecutionTreeType;
import de.uni_koblenz.west.koral.common.query.parser.SparqlParser;
import de.uni_koblenz.west.koral.common.query.parser.VariableDictionary;
import de.uni_koblenz.west.koral.common.utils.GraphFileFilter;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.koral.master.graph_cover_creator.GraphCoverCreator;
import de.uni_koblenz.west.koral.master.graph_cover_creator.NHopReplicator;
import de.uni_koblenz.west.koral.master.graph_cover_creator.impl.HashCoverCreator;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.koral.slave.triple_store.TripleStoreAccessor;

/**
 * A class to test source code. Not used within Koral.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class Playground {

  public static void main(String[] args) {
    if (args.length == 0) {
      System.out.println("Missing input file");
      return;
    }
    File workingDir = new File(System.getProperty("java.io.tmpdir") + File.separator + "koralTest");
    if (!workingDir.exists()) {
      workingDir.mkdir();
    }

    File inputFile = new File(args[0]);
    Configuration conf = new Configuration();

    GraphCoverCreator coverCreator = new HashCoverCreator(null, null);
    // GraphCoverCreator coverCreator = new HierarchicalCoverCreator(null,
    // null);
    // GraphCoverCreator coverCreator = new MinimalEdgeCutCover(null, null);

    // encode graph
    DictionaryEncoder encoder = new DictionaryEncoder(conf, null, null);
    File encodedInput = encoder.encodeOriginalGraphFiles(inputFile.isDirectory()
        ? inputFile.listFiles(new GraphFileFilter()) : new File[] {inputFile}, workingDir,
        coverCreator.getRequiredInputEncoding(), 4);

    // create cover
    File[] cover = coverCreator.createGraphCover(encoder, encodedInput, workingDir, 4);

    cover = encoder.encodeGraphChunksCompletely(cover, workingDir,
        coverCreator.getRequiredInputEncoding());

    NHopReplicator replicator = new NHopReplicator(null, null);
    cover = replicator.createNHopReplication(cover, workingDir, 0);

    // collect statistics
    GraphStatistics statistics = new GraphStatistics(conf, (short) 4, null);
    statistics.collectStatistics(cover);
    cover = statistics.adjustOwnership(cover, workingDir);

    System.out.println(statistics.toString());

    Playground.printContentOfChunks(cover, encoder, EncodingFileFormat.EEE);

    // store triples
    TripleStoreAccessor accessor = new TripleStoreAccessor(conf, null);
    for (File file : cover) {
      if (file != null) {
        accessor.storeTriples(file);
      }
    }

    // Playground.printQET(args, workingDir, conf, encoder, statistics,
    // accessor);

    encoder.close();
    statistics.close();
    accessor.close();

    Playground.delete(workingDir);
  }

  @SuppressWarnings("unused")
  private static void printQET(String[] args, File workingDir, Configuration conf,
      DictionaryEncoder encoder, GraphStatistics statistics, TripleStoreAccessor accessor) {
    // process query
    String query = Playground.readQueryFromFile(new File(args[1]));
    query = QueryFactory.create(query).serialize();
    System.out.println(query);

    VariableDictionary dictionary = new VariableDictionary();
    SparqlParser parser = new SparqlParser(encoder, statistics, accessor, (short) 0, 0, 0, 4,
        conf.getReceiverQueueSize(), workingDir, conf.getMaxEmittedMappingsPerRound(),
        conf.getJoinCacheStorageType(), conf.useTransactionsForJoinCache(),
        conf.isJoinCacheAsynchronouslyWritten(), conf.getJoinCacheType());
    QueryOperatorTask task = parser.parse(query, QueryExecutionTreeType.LEFT_LINEAR, dictionary);
    System.out.println(task.toString());

    QueryExecutionTreeDeserializer deserializer = new QueryExecutionTreeDeserializer(accessor,
        conf.getNumberOfSlaves(), conf.getReceiverQueueSize(), workingDir,
        conf.getJoinCacheStorageType(), conf.useTransactionsForJoinCache(),
        conf.isJoinCacheAsynchronouslyWritten(), conf.getJoinCacheType());

    for (int i = 0; i < 4; i++) {
      System.out.println("Slave " + i + ":");
      ((QueryOperatorBase) task).adjustEstimatedLoad(statistics, i);
      System.out.println(task.toString());
      byte[] serializedTask = ((QueryOperatorBase) task).serialize(false, 0);
      System.out.println();
      System.out.println(deserializer.deserialize(serializedTask));
    }
  }

  private static void printContentOfChunks(File[] encodedFiles, DictionaryEncoder encoder,
      EncodingFileFormat format) {
    for (File encodedFile : encodedFiles) {
      System.out.println("\nChunk " + encodedFile + "\n");
      if (encodedFile != null) {
        try (EncodedFileInputStream input = new EncodedFileInputStream(format, encodedFile);) {
          for (Statement statement : input) {
            System.out.println(" "
                + (statement.isSubjectEncoded() ? encoder.decode(statement.getSubjectAsLong())
                    : statement.getSubjectAsString())
                + " "
                + (statement.isPropertyEncoded() ? encoder.decode(statement.getPropertyAsLong())
                    : statement.getPropertyAsString())
                + " "
                + (statement.isObjectEncoded() ? encoder.decode(statement.getObjectAsLong())
                    : statement.getObjectAsString())
                + " " + Arrays.toString(statement.getContainment()));
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
    System.out.println();

  }

  static void delete(File dir) {
    for (File file : dir.listFiles()) {
      if (file.isDirectory()) {
        Playground.delete(file);
      } else {
        file.delete();
      }
    }
    dir.delete();
  }

  private static String readQueryFromFile(File queryFile) {
    try (BufferedReader br = new BufferedReader(new FileReader(queryFile));) {
      StringBuilder sb = new StringBuilder();
      String delim = "";
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        sb.append(delim);
        sb.append(line);
        delim = "\n";
      }
      return sb.toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
