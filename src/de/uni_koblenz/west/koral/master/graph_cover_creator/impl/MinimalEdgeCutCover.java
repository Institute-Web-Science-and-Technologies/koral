package de.uni_koblenz.west.koral.master.graph_cover_creator.impl;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedFileOutputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBDataStructureOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.measurement.MeasurementType;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.dictionary.Dictionary;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.koral.master.dictionary.impl.MapDBDictionary;
import de.uni_koblenz.west.koral.master.utils.AdjacencyMatrix;
import de.uni_koblenz.west.koral.master.utils.DeSerializer;
import de.uni_koblenz.west.koral.master.utils.LongIterator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Scanner;
import java.util.logging.Logger;

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
  public EncodingFileFormat getRequiredInputEncoding() {
    return EncodingFileFormat.EEE;
  }

  @Override
  protected void createCover(DictionaryEncoder dictionary, EncodedFileInputStream input,
          int numberOfGraphChunks, EncodedFileOutputStream[] outputs, boolean[] writtenFiles,
          File workingDir) {
    File dictionaryFolder = new File(
            workingDir.getAbsolutePath() + File.separator + "minEdgeCutDictionary");
    if (!dictionaryFolder.exists()) {
      dictionaryFolder.mkdirs();
    }
    // TODO use a long2long dictionary
    Dictionary localDictionary = new MapDBDictionary(MapDBStorageOptions.MEMORY_MAPPED_FILE,
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
        createMetisInputFile(dictionary, input, localDictionary, encodedRDFGraph, metisInputGraph,
                ignoredTriples, workingDir);
        metisOutputGraph = runMetis(metisInputGraph, numberOfGraphChunks);
      } finally {
        metisInputGraph.delete();
      }

      createGraphCover(encodedRDFGraph, localDictionary, metisOutputGraph, ignoredTriples, outputs,
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
      localDictionary.close();
      deleteFolder(dictionaryFolder);
    }
  }

  private void createMetisInputFile(DictionaryEncoder dictionary, EncodedFileInputStream input,
          Dictionary localDictionary, File encodedRDFGraph, File metisInputGraph,
          File ignoredTriples, File workingDir) {
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

    long encodedRdfTypeLabel = dictionary.encodeWithoutOwnership(
            DeSerializer.deserializeNode("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"),
            false);

    long numberOfVertices = 0;
    long numberOfEdges = 0;
    long numberOfUsedTriples = 0;
    long numberOfIgnoredTriples = 0;

    AdjacencyMatrix adjacencyMatrix = new AdjacencyMatrix(metisInputTempFolder);
    // create adjacency lists
    try {
      try (EncodedFileOutputStream encodedGraphOutput = new EncodedFileOutputStream(
              encodedRDFGraph);
              EncodedFileOutputStream ignoredTriplesOutput = new EncodedFileOutputStream(
                      ignoredTriples);) {
        for (Statement statement : input) {
          if (Arrays.equals(statement.getSubject(), statement.getObject())
                  || (statement.getPropertyAsLong() == encodedRdfTypeLabel)) {
            // this is a self loop that is forbidden in METIS or
            // it is a rdf:type triple
            // store it as ignored triple
            numberOfIgnoredTriples++;
            ignoredTriplesOutput.writeStatement(statement);
            continue;
          }
          numberOfUsedTriples++;
          long encodedSubject = localDictionary.encode(Long.toString(statement.getSubjectAsLong()),
                  true);
          if (encodedSubject > numberOfVertices) {
            numberOfVertices = encodedSubject;
          }
          long encodedObject = localDictionary.encode(Long.toString(statement.getObjectAsLong()),
                  true);
          if (encodedObject > numberOfVertices) {
            numberOfVertices = encodedObject;
          }
          // write encoded triple to graph file
          Statement encodedStatement = Statement.getStatement(getRequiredInputEncoding(),
                  NumberConversion.long2bytes(encodedSubject), statement.getProperty(),
                  NumberConversion.long2bytes(encodedObject), statement.getContainment());
          encodedGraphOutput.writeStatement(encodedStatement);

          adjacencyMatrix.addEdge(encodedSubject, encodedObject);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      numberOfVertices = adjacencyMatrix.getNumberOfVertices();
      numberOfEdges = adjacencyMatrix.getNumberOfEdges();

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
          metisInputGraphWriter.write("\n");
          String delim = "";
          LongIterator iterator = adjacencyMatrix.getAdjacencyList(vertex);
          while (iterator.hasNext()) {
            long neighbour = iterator.next();
            metisInputGraphWriter.write(delim + neighbour);
            delim = " ";
          }
          iterator.close();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

    } finally {
      adjacencyMatrix.close();
      deleteFolder(metisInputTempFolder);
      if (measurementCollector != null) {
        measurementCollector.measureValue(
                MeasurementType.LOAD_GRAPH_COVER_CREATION_METIS_INPUT_FILE_CREATION_END,
                System.currentTimeMillis());
      }
    }
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
          File ignoredTriples, EncodedFileOutputStream[] outputs, boolean[] writtenFiles,
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
      try (EncodedFileInputStream graphInput = new EncodedFileInputStream(
              getRequiredInputEncoding(), encodedRDFGraph);) {
        long lastVertex = -1;
        int lastChunkIndex = -1;

        for (Statement statement : graphInput) {
          long subject = Long.parseLong(dictionary.decode(statement.getSubjectAsLong()));
          long object = Long.parseLong(dictionary.decode(statement.getObjectAsLong()));
          Statement newStatement = Statement.getStatement(getRequiredInputEncoding(),
                  NumberConversion.long2bytes(subject), statement.getProperty(),
                  NumberConversion.long2bytes(object), statement.getContainment());

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

          writeStatementToChunk(targetChunk, numberOfGraphChunks, newStatement, outputs,
                  writtenFiles);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      if (ignoredTriples.exists()) {
        // assign ignored triples
        try (EncodedFileInputStream graphInput = new EncodedFileInputStream(
                getRequiredInputEncoding(), ignoredTriples);) {
          long lastVertex = -1;
          int lastChunkIndex = -1;

          for (Statement statement : graphInput) {
            long subject = statement.getSubjectAsLong();

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
      Path path = FileSystems.getDefault().getPath(folder.getAbsolutePath());
      try {
        Files.walkFileTree(path, new FileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                  throws IOException {
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                  throws IOException {
            // here you have the files to process
            file.toFile().delete();
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            return FileVisitResult.TERMINATE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
          }
        });
      } catch (IOException e) {
        throw new RuntimeException(e);
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
