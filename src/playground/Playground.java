package playground;

import org.apache.jena.query.QueryFactory;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.query.Mapping;
import de.uni_koblenz.west.koral.common.query.MappingRecycleCache;
import de.uni_koblenz.west.koral.common.query.TriplePattern;
import de.uni_koblenz.west.koral.common.query.TriplePatternType;
import de.uni_koblenz.west.koral.common.query.execution.QueryExecutionTreeDeserializer;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorBase;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.koral.common.query.parser.QueryExecutionTreeType;
import de.uni_koblenz.west.koral.common.query.parser.SparqlParser;
import de.uni_koblenz.west.koral.common.query.parser.VariableDictionary;
import de.uni_koblenz.west.koral.common.utils.RDFFileIterator;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.koral.master.graph_cover_creator.GraphCoverCreator;
import de.uni_koblenz.west.koral.master.graph_cover_creator.NHopReplicator;
import de.uni_koblenz.west.koral.master.graph_cover_creator.impl.HashCoverCreator;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.koral.slave.triple_store.TripleStoreAccessor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

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
    conf.setDictionaryDir(workingDir.getAbsolutePath() + File.separator + "dictionary");
    conf.setStatisticsDir(workingDir.getAbsolutePath() + File.separator + "statistics");

    // create cover
    RDFFileIterator iterator = new RDFFileIterator(inputFile, false, null);
    GraphCoverCreator coverCreator = new HashCoverCreator(null, null);
    // GraphCoverCreator coverCreator = new HierarchicalCoverCreator(null,
    // null);
    // GraphCoverCreator coverCreator = new MinimalEdgeCutCover(null, null);
    // TODO METIS bug
    File[] cover = coverCreator.createGraphCover(iterator, workingDir, 4);

    NHopReplicator replicator = new NHopReplicator(null, null);
    cover = replicator.createNHopReplication(cover, workingDir, 0);

    // encode cover and collect statistics
    DictionaryEncoder encoder = new DictionaryEncoder(conf, null, null);
    File[] encodedFiles = encoder.encodeGraphChunks(cover, workingDir);
    GraphStatistics statistics = new GraphStatistics(conf, (short) 4, null);
    statistics.collectStatistics(encodedFiles);

    System.out.println(statistics.toString());

    Playground.printContentOfChunks(encodedFiles, encoder, conf, workingDir);

    // store triples
    conf.setTripleStoreDir(workingDir.getAbsolutePath() + File.separator + "tripleStore");
    TripleStoreAccessor accessor = new TripleStoreAccessor(conf, null);
    for (File file : encodedFiles) {
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
          Configuration conf, File workingDir) {

    TripleStoreAccessor[] accessors = new TripleStoreAccessor[encodedFiles.length];
    for (int i = 0; i < encodedFiles.length; i++) {
      if (encodedFiles[i] != null) {
        conf.setTripleStoreDir(
                workingDir.getAbsolutePath() + File.separator + "tripleStore_chunk" + i);
        accessors[i] = new TripleStoreAccessor(conf, null);
        accessors[i].storeTriples(encodedFiles[i]);
      }
    }

    MappingRecycleCache cache = new MappingRecycleCache(10, 4);
    long[] resultVars = new long[] { 0, 1, 2 };
    for (int i = 0; i < accessors.length; i++) {
      if (accessors[i] != null) {
        System.out.println("\nChunk " + i + "\n");
        TripleStoreAccessor accessor = accessors[i];
        for (Mapping result : accessor.lookup(cache,
                new TriplePattern(TriplePatternType.___, 0, 1, 2))) {
          System.out.print(result.printContainment());
          for (long var : resultVars) {
            System.out.print(" " + encoder.decode(result.getValue(var, resultVars)));
          }
          System.out.println();
        }
        accessor.close();
      }
    }

  }

  private static void delete(File dir) {
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
