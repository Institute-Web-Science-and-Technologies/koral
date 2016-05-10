package de.uni_koblenz.west.koral.master.graph_cover_creator.impl;

import org.apache.jena.graph.Node;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBDataStructureOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.measurement.MeasurementType;
import de.uni_koblenz.west.koral.common.utils.RDFFileIterator;
import de.uni_koblenz.west.koral.master.dictionary.Dictionary;
import de.uni_koblenz.west.koral.master.dictionary.impl.MapDBDictionary;
import de.uni_koblenz.west.koral.master.utils.DeSerializer;
import de.uni_koblenz.west.koral.master.utils.FileLongSet;
import de.uni_koblenz.west.koral.master.utils.LongIterator;

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

  public MinimalEdgeCutCover(Logger logger, MeasurementCollector measurementCollector) {
    super(logger, measurementCollector);
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
        ignoredTriples.delete();
      }
      dictionary.close();
      deleteFolder(dictionaryFolder);
    }
  }

  private void createMetisInputFile(RDFFileIterator rdfFiles, Dictionary dictionary,
          File encodedRDFGraph, File metisInputGraph, File ignoredTriples, File workingDir) {
    if (measurementCollector != null) {
      measurementCollector.measureValue(
              MeasurementType.LOAD_GRAPH_COVER_CREATION_METIS_INPUT_FILE_CREATION_START,
              System.currentTimeMillis());
    }
    File metisInputTempFolder = new File(
            workingDir.getAbsolutePath() + File.separator + "metisInputCreation");
    if (!metisInputTempFolder.exists()) {
      metisInputTempFolder.mkdirs();
    }

    long numberOfVertices = 0;
    long numberOfEdges = 0;
    long numberOfUsedTriples = 0;
    long numberOfIgnoredTriples = 0;

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
            // this is a self loop that is forbidden in METIS or
            // it is a rdf:type triple
            // store it as ignored triple
            numberOfIgnoredTriples++;
            byte[] subject = DeSerializer.serializeNode(statement[0]).getBytes("UTF-8");
            byte[] property = DeSerializer.serializeNode(statement[1]).getBytes("UTF-8");
            byte[] object = DeSerializer.serializeNode(statement[2]).getBytes("UTF-8");
            ignoredTriplesOutput.writeInt(subject.length);
            ignoredTriplesOutput.write(subject);
            ignoredTriplesOutput.writeInt(property.length);
            ignoredTriplesOutput.write(property);
            ignoredTriplesOutput.writeInt(object.length);
            ignoredTriplesOutput.write(object);
            continue;
          }
          numberOfUsedTriples++;
          long encodedSubject = dictionary.encode(DeSerializer.serializeNode(statement[0]), true);
          if (encodedSubject > numberOfVertices) {
            numberOfVertices = encodedSubject;
          }
          long encodedObject = dictionary.encode(DeSerializer.serializeNode(statement[2]), true);
          if (encodedObject > numberOfVertices) {
            numberOfVertices = encodedObject;
          }
          // write encoded triple to graph file
          encodedGraphOutput.writeLong(encodedSubject);
          String property = DeSerializer.serializeNode(statement[1]);
          encodedGraphOutput.writeInt(property.length());
          encodedGraphOutput.writeBytes(property);
          encodedGraphOutput.writeLong(encodedObject);
          // add direction subject2object
          FileLongSet adjacentVerticesOfSubject = new FileLongSet(
                  getAdjacencyListFile(metisInputTempFolder, encodedSubject));
          adjacentVerticesOfSubject.close();
          boolean isNewAdjacency = adjacentVerticesOfSubject.add(encodedObject);
          adjacentVerticesOfSubject.close();
          if (isNewAdjacency) {
            numberOfEdges++;
          }
          // add direction object2subject (since edges are bidirectional, do not
          // count an additional edge)
          FileLongSet adjacentVerticesOfObject = new FileLongSet(
                  getAdjacencyListFile(metisInputTempFolder, encodedObject));
          adjacentVerticesOfObject.add(encodedSubject);
          adjacentVerticesOfObject.close();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      if (measurementCollector != null) {
        measurementCollector.measureValue(
                MeasurementType.LOAD_GRAPH_COVER_CREATION_METIS_IGNORED_TRIPLES,
                numberOfIgnoredTriples);
        measurementCollector.measureValue(
                MeasurementType.LOAD_GRAPH_COVER_CREATION_METIS_INPUT_GRAPH_SIZE,
                Long.toString(numberOfUsedTriples), Long.toString(numberOfVertices),
                Long.toString(numberOfEdges));
      }

      // write adjacency lists to file
      try (BufferedWriter metisInputGraphWriter = new BufferedWriter(
              new OutputStreamWriter(new FileOutputStream(metisInputGraph), "UTF-8"));) {
        metisInputGraphWriter.write(numberOfVertices + " " + numberOfEdges);
        for (long vertex = 1; vertex <= numberOfVertices; vertex++) {
          FileLongSet adjacentVertices = new FileLongSet(
                  getAdjacencyListFile(metisInputTempFolder, vertex));
          metisInputGraphWriter.write("\n");
          String delim = "";
          LongIterator iterator = adjacentVertices.iterator();
          while (iterator.hasNext()) {
            long neighbour = iterator.next();
            metisInputGraphWriter.write(delim + neighbour);
            delim = " ";
          }
          iterator.close();
          adjacentVertices.close();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

    } finally {
      deleteFolder(metisInputTempFolder);
      if (measurementCollector != null) {
        measurementCollector.measureValue(
                MeasurementType.LOAD_GRAPH_COVER_CREATION_METIS_INPUT_FILE_CREATION_END,
                System.currentTimeMillis());
      }
    }
  }

  private File getAdjacencyListFile(File adjacencyListFolder, long adjacencyListId) {
    return new File(adjacencyListFolder + File.separator + adjacencyListId);
  }

  private File runMetis(File metisInputGraph, int numberOfGraphChunks) {
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_COVER_CREATION_RUN_METIS_START,
              System.currentTimeMillis());
    }
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
      if (measurementCollector != null) {
        measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_COVER_CREATION_RUN_METIS_END,
                System.currentTimeMillis());
      }
    }
  }

  private void createGraphCover(File encodedRDFGraph, Dictionary dictionary, File metisOutputGraph,
          File ignoredTriples, OutputStream[] outputs, boolean[] writtenFiles,
          int numberOfGraphChunks, File workingDir) {
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_COVER_CREATION_FILE_WRITE_START,
              System.currentTimeMillis());
    }
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
            String subjectString = new String(subjectByteArray, "UTF-8");
            long subject = dictionary.encode(subjectString, false);

            int propertyLength = graphInput.readInt();
            byte[] propertyString = new byte[propertyLength];
            graphInput.readFully(propertyString);
            String property = new String(propertyString, "UTF-8");

            int objectLength = graphInput.readInt();
            byte[] objectByteArray = new byte[objectLength];
            graphInput.readFully(objectByteArray);
            String objectString = new String(objectByteArray, "UTF-8");

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
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_COVER_CREATION_FILE_WRITE_END,
              System.currentTimeMillis());
    }
  }

  private void deleteFolder(File folder) {
    if (!folder.exists()) {
      return;
    }
    if (folder.isDirectory()) {
      for (File file : folder.listFiles()) {
        deleteFolder(file);
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

}
