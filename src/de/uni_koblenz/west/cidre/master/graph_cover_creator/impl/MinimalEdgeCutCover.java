package de.uni_koblenz.west.cidre.master.graph_cover_creator.impl;

import org.apache.jena.graph.Node;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import de.uni_koblenz.west.cidre.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.cidre.common.mapDB.MapDBDataStructureOptions;
import de.uni_koblenz.west.cidre.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;
import de.uni_koblenz.west.cidre.master.dictionary.Dictionary;
import de.uni_koblenz.west.cidre.master.dictionary.impl.MapDBDictionary;
import de.uni_koblenz.west.cidre.master.utils.DeSerializer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Creates a minimal edge-cut cover with the help of
 * <a href="http://glaros.dtc.umn.edu/gkhome/metis/metis/overview">METIS</a>
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MinimalEdgeCutCover extends GraphCoverCreatorBase {

  private Process process;

  public MinimalEdgeCutCover(Logger logger) {
    super(logger);
  }

  @Override
  protected void createCover(RDFFileIterator rdfFiles, int numberOfGraphChunks,
          OutputStream[] outputs, boolean[] writtenFiles, File workingDir) {
    File dictionaryFolder = new File(
            workingDir.getAbsolutePath() + File.separator + "minEdgeCutDictionary");
    if (!dictionaryFolder.exists()) {
      dictionaryFolder.mkdirs();
    }
    Dictionary dictionary = new MapDBDictionary(MapDBStorageOptions.MEMORY_MAPPED_FILE,
            MapDBDataStructureOptions.HASH_TREE_MAP, dictionaryFolder.getAbsolutePath(), false,
            true, MapDBCacheOptions.HASH_TABLE);

    File encodedRDFGraph = null;
    File metisOutputGraph = null;
    File ignoredTriples = new File(
            workingDir.getAbsolutePath() + File.separator + "ignoredTriples.gz");
    try {
      encodedRDFGraph = new File(
              workingDir.getAbsolutePath() + File.separator + "encodedRDFGraph.gz");
      File metisInputGraph = new File(workingDir.getAbsolutePath() + File.separator + "metisInput");

      try {
        createMetisInputFile(rdfFiles, dictionary, encodedRDFGraph, metisInputGraph, ignoredTriples,
                workingDir);
        metisOutputGraph = runMetis(metisInputGraph, numberOfGraphChunks);
      } finally {
        metisInputGraph.delete();
      }

      createGraphCover(encodedRDFGraph, dictionary, metisOutputGraph, ignoredTriples, outputs,
              writtenFiles, numberOfGraphChunks, workingDir);
    } finally {
      if (encodedRDFGraph != null) {
        encodedRDFGraph.delete();
      }
      if (metisOutputGraph != null) {
        metisOutputGraph.delete();
      }
      if (ignoredTriples.exists()) {
        // TODO enable again
        // ignoredTriples.delete();
      }
      dictionary.close();
      deleteFolder(dictionaryFolder);
    }
  }

  private void createMetisInputFile(RDFFileIterator rdfFiles, Dictionary dictionary,
          File encodedRDFGraph, File metisInputGraph, File ignoredTriples, File workingDir) {
    File metisInputTempFolder = new File(
            workingDir.getAbsolutePath() + File.separator + "metisInputCreation");
    if (!metisInputTempFolder.exists()) {
      metisInputTempFolder.mkdirs();
    }
    DBMaker<?> dbmaker = MapDBStorageOptions.MEMORY_MAPPED_FILE
            .getDBMaker(
                    metisInputTempFolder.getAbsolutePath() + File.separator + "metisInputCreation")
            .transactionDisable().closeOnJvmShutdown().asyncWriteEnable();
    dbmaker = MapDBCacheOptions.HASH_TABLE.setCaching(dbmaker);
    DB database = dbmaker.make();

    long numberOfVertices = 0;
    long numberOfEdges = 0;
    HTreeMap<Long, Set<Long>> adjacencyLists = database.createHashMap("adjacenceList").makeOrGet();

    // create adjacency lists
    try {
      try (DataOutputStream encodedGraphOutput = new DataOutputStream(new BufferedOutputStream(
              new GZIPOutputStream(new FileOutputStream(encodedRDFGraph))));
              DataOutputStream ignoredTriplesOutput = new DataOutputStream(new BufferedOutputStream(
                      new GZIPOutputStream(new FileOutputStream(ignoredTriples))))) {
        for (Node[] statement : rdfFiles) {
          transformBlankNodes(statement);
          if (statement[0].equals(statement[2]) || DeSerializer.serializeNode(statement[1])
                  .equals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")) {
            // this is a self loop that is forbidden in METIS
            // assign it to the first chunk
            String subject = DeSerializer.serializeNode(statement[0]);
            String property = DeSerializer.serializeNode(statement[1]);
            String object = DeSerializer.serializeNode(statement[2]);
            ignoredTriplesOutput.writeInt(subject.length());
            ignoredTriplesOutput.writeBytes(subject);
            ignoredTriplesOutput.writeInt(property.length());
            ignoredTriplesOutput.writeBytes(property);
            ignoredTriplesOutput.writeInt(object.length());
            ignoredTriplesOutput.writeBytes(object);
            continue;
          }
          long encodedSubject = dictionary.encode(DeSerializer.serializeNode(statement[0]), true);
          long encodedObject = dictionary.encode(DeSerializer.serializeNode(statement[2]), true);
          // write encoded triple to graph file
          encodedGraphOutput.writeLong(encodedSubject);
          String property = DeSerializer.serializeNode(statement[1]);
          encodedGraphOutput.writeInt(property.length());
          encodedGraphOutput.writeBytes(property);
          encodedGraphOutput.writeLong(encodedObject);
          // ignore loops since METIS does not allow them
          // if (encodedSubject == encodedObject) {
          // continue;
          // }
          // add direction subject2object
          Set<Long> adjacentVerticesOfSubject = adjacencyLists.get(encodedSubject);
          if (adjacentVerticesOfSubject == null) {
            numberOfVertices++;
            adjacentVerticesOfSubject = new ConcurrentSkipListSet<>();
          }
          boolean isNewAdjacency = adjacentVerticesOfSubject.add(encodedObject);
          if (isNewAdjacency) {
            numberOfEdges++;
            adjacencyLists.put(encodedSubject, adjacentVerticesOfSubject);
          }
          // add direction object2subject (since edges are bidirectional, do not
          // count an additional edge)
          Set<Long> adjacentVerticesOfObject = adjacencyLists.get(encodedObject);
          if (adjacentVerticesOfObject == null) {
            numberOfVertices++;
            adjacentVerticesOfObject = new ConcurrentSkipListSet<>();
          }
          boolean setHasChanged = adjacentVerticesOfObject.add(encodedSubject);
          if (setHasChanged) {
            adjacencyLists.put(encodedObject, adjacentVerticesOfObject);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      // write adjacency lists to file
      try (BufferedWriter metisInputGraphWriter = new BufferedWriter(
              new OutputStreamWriter(new FileOutputStream(metisInputGraph), "UTF-8"));) {
        metisInputGraphWriter.write(numberOfVertices + " " + numberOfEdges);
        for (long vertex = 1; vertex <= numberOfVertices; vertex++) {
          Set<Long> adjacentVertices = adjacencyLists.get(vertex);
          metisInputGraphWriter.write("\n");
          String delim = "";
          for (Long neighbour : adjacentVertices) {
            metisInputGraphWriter.write(delim + neighbour.longValue());
            delim = " ";
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

    } finally {
      database.close();
      deleteFolder(metisInputTempFolder);
    }
  }

  private File runMetis(File metisInputGraph, int numberOfGraphChunks) {
    ProcessBuilder processBuilder = new ProcessBuilder("gpmetis", metisInputGraph.getAbsolutePath(),
            new Integer(numberOfGraphChunks).toString());
    if (logger == null) {
      processBuilder.inheritIO();
    }
    try {
      process = processBuilder.start();
      if (logger != null) {
        new LogPipedWriter(process.getInputStream(), logger).start();
        new LogPipedWriter(process.getErrorStream(), logger).start();
      }
      process.waitFor();
      process = null;
      return new File(metisInputGraph.getAbsolutePath() + ".part." + numberOfGraphChunks);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      if ((process != null) && process.isAlive()) {
        process.destroy();
      }
    }
  }

  private void createGraphCover(File encodedRDFGraph, Dictionary dictionary, File metisOutputGraph,
          File ignoredTriples, OutputStream[] outputs, boolean[] writtenFiles,
          int numberOfGraphChunks, File workingDir) {
    File vertex2chunkIndexFolder = new File(
            workingDir.getAbsolutePath() + File.separator + "vertex2chunkIndex");
    if (!vertex2chunkIndexFolder.exists()) {
      vertex2chunkIndexFolder.mkdirs();
    }
    DBMaker<?> dbmaker = MapDBStorageOptions.MEMORY_MAPPED_FILE
            .getDBMaker(vertex2chunkIndexFolder.getAbsolutePath() + File.separator
                    + "vertex2chunkIndex")
            .transactionDisable().closeOnJvmShutdown().asyncWriteEnable();
    dbmaker = MapDBCacheOptions.HASH_TABLE.setCaching(dbmaker);
    DB database = dbmaker.make();

    try {
      // load mapping vertex -> chunkIndex from metis output
      HTreeMap<Long, Integer> vertex2chunkIndex = database.createHashMap("vertex2chunkIndex")
              .makeOrGet();
      try (Scanner scanner = new Scanner(metisOutputGraph);) {
        scanner.useDelimiter("\\r?\\n");
        long vertex = 1;
        while (scanner.hasNextInt()) {
          int chunkIndex = scanner.nextInt();
          vertex2chunkIndex.put(vertex, chunkIndex);
          vertex++;
        }
      } catch (FileNotFoundException e) {
        throw new RuntimeException(e);
      }

      // assign partitioned triples
      try (DataInputStream graphInput = new DataInputStream(new BufferedInputStream(
              new GZIPInputStream(new FileInputStream(encodedRDFGraph))));) {
        long lastVertex = -1;
        int lastChunkIndex = -1;

        while (true) {
          long subject = graphInput.readLong();
          String subjectString = dictionary.decode(subject);

          int propertyLength = graphInput.readInt();
          byte[] propertyString = new byte[propertyLength];
          graphInput.readFully(propertyString);
          String property = new String(propertyString);

          long object = graphInput.readLong();
          String objectString = dictionary.decode(object);

          Node[] statement = new Node[] { DeSerializer.deserializeNode(subjectString),
                  DeSerializer.deserializeNode(property),
                  DeSerializer.deserializeNode(objectString) };

          int targetChunk = -1;
          if (subject == lastVertex) {
            targetChunk = lastChunkIndex;
          } else {
            Integer target = vertex2chunkIndex.get(subject);
            if (target == null) {
              // if a vertex only occurs in a self loop or with a rdf:type
              // property it is not partitioned by
              // metis
              target = 0;
            }
            targetChunk = target;
            lastVertex = subject;
            lastChunkIndex = targetChunk;
          }

          writeStatementToChunk(targetChunk, numberOfGraphChunks, statement, outputs, writtenFiles);
        }
      } catch (EOFException e1) {
        // when encoded graph file is completely processed, this exception is
        // thrown
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      if (ignoredTriples.exists()) {
        // assign ignored triples
        try (DataInputStream graphInput = new DataInputStream(new BufferedInputStream(
                new GZIPInputStream(new FileInputStream(ignoredTriples))));) {
          long lastVertex = -1;
          int lastChunkIndex = -1;

          while (true) {
            int subjectLength = graphInput.readInt();
            byte[] subjectByteArray = new byte[subjectLength];
            graphInput.readFully(subjectByteArray);
            String subjectString = new String(subjectByteArray);
            long subject = dictionary.encode(subjectString, false);

            int propertyLength = graphInput.readInt();
            byte[] propertyString = new byte[propertyLength];
            graphInput.readFully(propertyString);
            String property = new String(propertyString);

            int objectLength = graphInput.readInt();
            byte[] objectByteArray = new byte[objectLength];
            graphInput.readFully(objectByteArray);
            String objectString = new String(objectByteArray);

            Node[] statement = new Node[] { DeSerializer.deserializeNode(subjectString),
                    DeSerializer.deserializeNode(property),
                    DeSerializer.deserializeNode(objectString) };

            int targetChunk = -1;
            if (subject == lastVertex) {
              targetChunk = lastChunkIndex;
            } else {
              Integer target = vertex2chunkIndex.get(subject);
              if (target == null) {
                // if a vertex only occurs in a self loop or with a rdf:type
                // property it is not partitioned by
                // metis
                target = 0;
              }
              targetChunk = target;
              lastVertex = subject;
              lastChunkIndex = targetChunk;
            }

            writeStatementToChunk(targetChunk, numberOfGraphChunks, statement, outputs,
                    writtenFiles);
          }
        } catch (EOFException e1) {
          // when encoded graph file is completely processed, this exception is
          // thrown
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

    } finally {
      database.close();
      deleteFolder(vertex2chunkIndexFolder);
    }
  }

  private void deleteFolder(File folder) {
    if (!folder.exists()) {
      return;
    }
    if (folder.isDirectory()) {
      for (File file : folder.listFiles()) {
        file.delete();
      }
    }
    folder.delete();
  }

  @Override
  public void close() {
    if ((process != null) && process.isAlive()) {
      process.destroy();
    }
    super.close();
  }

  public static void main(String[] args) {
    // TODO fix bug during determining number of edges
    RDFFileIterator iterator = new RDFFileIterator(
            new File("/home/danijank/Downloads/cleaned_01data-3-00.nq.gz"), false, null);
    MinimalEdgeCutCover cover = new MinimalEdgeCutCover(null);
    cover.createGraphCover(iterator, new File("/tmp"), 4);
  }

}
