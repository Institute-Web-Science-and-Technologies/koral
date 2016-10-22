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
package de.uni_koblenz.west.koral.master.graph_cover_creator;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedFileOutputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.measurement.MeasurementType;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.utils.MoleculeListIterator;
import de.uni_koblenz.west.koral.master.utils.MoleculeLists;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Set;
import java.util.logging.Logger;

public class NHopReplicator {

  private final Logger logger;

  private final MeasurementCollector measurementCollector;

  public NHopReplicator(Logger logger, MeasurementCollector measurementCollector) {
    this.logger = logger;
    this.measurementCollector = measurementCollector;
  }

  public File[] createNHopReplication(File[] graphCover, File workingDir, int numberOfHops) {
    if (logger != null) {
      logger.info("Starting " + numberOfHops + "-hop replication.");
    }

    File[] nHopReplicatedFiles = null;

    if (numberOfHops > 0) {
      File mapFolder = new File(workingDir.getAbsolutePath() + File.separator + "nHopReplication");
      if (!mapFolder.exists()) {
        mapFolder.mkdirs();
      }
      DBMaker<?> dbmaker = MapDBStorageOptions.MEMORY_MAPPED_FILE
              .getDBMaker(mapFolder.getAbsolutePath() + File.separator + "nHopReplication")
              .transactionDisable().closeOnJvmShutdown().asyncWriteEnable();
      dbmaker = MapDBCacheOptions.HASH_TABLE.setCaching(dbmaker).compressionEnable();
      DB database = dbmaker.make();

      try {
        MoleculeLists moleculeLists = new MoleculeLists(mapFolder);
        Set<Long>[] cover = createInitialCover(database, moleculeLists, graphCover, mapFolder);

        // perform n-hop replication
        for (int n = 1; n <= numberOfHops; n++) {
          if (logger != null) {
            logger.info("Performing " + n + "-hop replication");
          }
          performHopStep(database, cover, moleculeLists, n);
        }

        // update containment information
        if (measurementCollector != null) {
          measurementCollector.measureValue(
                  MeasurementType.LOAD_GRAPH_NHOP_REPLICATION_CONTAINMENT_UPDATE_START,
                  System.currentTimeMillis());
        }
        for (int i = 0; i < cover.length; i++) {
          // TODO remove
          System.out.println("\tadjust containment " + i);
          adjustContainment(i, cover[i], moleculeLists);
        }
        if (measurementCollector != null) {
          measurementCollector.measureValue(
                  MeasurementType.LOAD_GRAPH_NHOP_REPLICATION_CONTAINMENT_UPDATE_END,
                  System.currentTimeMillis());
        }

        nHopReplicatedFiles = convertToFiles(cover, moleculeLists, workingDir);
        for (File file : graphCover) {
          file.delete();
        }
        moleculeLists.close();
      } finally {
        // clean up
        database.close();
        deleteFolder(mapFolder);
      }
    } else {
      nHopReplicatedFiles = graphCover;
    }

    if (logger != null) {
      logger.info("Finished " + numberOfHops + "-hop replication.");
    }
    return nHopReplicatedFiles;
  }

  private Set<Long>[] createInitialCover(DB database, MoleculeLists moleculeLists,
          File[] graphCover, File mapFolder) {
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_NHOP_REPLICATION_INIT_START,
              System.currentTimeMillis());
    }
    @SuppressWarnings("unchecked")
    Set<Long>[] subjectSets = new Set[graphCover.length];
    for (int i = 0; i < graphCover.length; i++) {
      if (logger != null) {
        logger.info("preprocessiong chunk " + i);
      }
      subjectSets[i] = createMapOutOfFile(database, mapFolder, moleculeLists, graphCover[i], i);
    }
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_NHOP_REPLICATION_INIT_END,
              System.currentTimeMillis());
    }
    return subjectSets;
  }

  private Set<Long> createMapOutOfFile(DB database, File mapFolder, MoleculeLists moleculeLists,
          File file, int chunkNumber) {
    if (file == null) {
      return null;
    }
    Set<Long> subjectSet = database.createHashSet("subjectsOfChunk" + chunkNumber).makeOrGet();
    try (EncodedFileInputStream input = new EncodedFileInputStream(EncodingFileFormat.EEE, file);) {
      for (Statement statement : input) {
        moleculeLists.add(statement);
        subjectSet.add(statement.getSubjectAsLong());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return subjectSet;
  }

  private void performHopStep(DB database, Set<Long>[] cover, MoleculeLists moleculeLists,
          int hopNumber) {
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_NHOP_REPLICATION_STEP_START,
              System.currentTimeMillis(), Integer.toString(hopNumber));
    }
    Set<Long> tmpSet = database.getHashSet("tempSubjectSet");
    // TODO remove
    System.out.println("perform hop step " + hopNumber);
    for (int currentCoverIndex = 0; currentCoverIndex < cover.length; currentCoverIndex++) {
      if (logger != null) {
        logger.info("performing hop " + hopNumber + " for chunk " + currentCoverIndex);
      }
      // TODO remove
      System.out.println("\tchunk " + currentCoverIndex);
      if (currentCoverIndex != 0) {
        tmpSet.clear();
      }
      replicateTriples(database, cover[currentCoverIndex], moleculeLists, tmpSet);
    }
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_NHOP_REPLICATION_STEP_END,
              System.currentTimeMillis(), Integer.toString(hopNumber));
    }
  }

  private void replicateTriples(DB database, Set<Long> chunk, MoleculeLists moleculeLists,
          Set<Long> tmpSet) {
    if (chunk == null) {
      return;
    }
    for (Long subject : chunk) {
      MoleculeListIterator iterator = moleculeLists.iterator(subject, true);
      while (iterator.hasNext()) {
        byte[][] statement = iterator.next();
        // add object to current chunk
        tmpSet.add(NumberConversion.bytes2long(statement[2]));
      }
      iterator.close();
    }
    for (Long subject : tmpSet) {
      chunk.add(subject);
    }
  }

  private void adjustContainment(int currentChunkIndex, Set<Long> chunk,
          MoleculeLists moleculeLists) {
    if (chunk == null) {
      return;
    }
    if (logger != null) {
      logger.finer("adjusting containment of chunk " + currentChunkIndex);
    }
    for (Long subject : chunk) {
      MoleculeListIterator iterator = moleculeLists.iterator(subject, false);
      while (iterator.hasNext()) {
        byte[][] statement = iterator.next();
        iterator.updateContainment(statement, currentChunkIndex);
      }
      iterator.close();
    }
  }

  private File[] convertToFiles(Set<Long>[] cover, MoleculeLists moleculeLists, File workingDir) {
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_NHOP_REPLICATION_FILEWRITE_START,
              System.currentTimeMillis());
    }
    File[] chunks = new File[cover.length];
    long[] numberOfTriples = new long[cover.length];
    for (int i = 0; i < cover.length; i++) {
      chunks[i] = convertToFile(cover[i], i, moleculeLists, workingDir, numberOfTriples);
    }
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_NHOP_REPLICATION_FILEWRITE_END,
              System.currentTimeMillis());
      String[] triplesPerChunk = new String[cover.length];
      for (int i = 0; i < numberOfTriples.length; i++) {
        triplesPerChunk[i] = Long.toString(numberOfTriples[i]);
      }
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_REPLICATED_CHUNK_SIZES,
              triplesPerChunk);
    }
    return chunks;
  }

  private File convertToFile(Set<Long> subjects, int chunkIndex, MoleculeLists moleculeLists,
          File workingDir, long[] numberOfTriples) {
    if (subjects == null) {
      return null;
    }
    if (logger != null) {
      logger.finer("Converting chunk " + chunkIndex + " into a file.");
    }
    File chunkFile = getFile(chunkIndex, workingDir);
    try (EncodedFileOutputStream output = new EncodedFileOutputStream(chunkFile);) {
      for (Long subject : subjects) {
        MoleculeListIterator iterator = moleculeLists.iterator(subject, true);
        while (iterator.hasNext()) {
          byte[][] triple = iterator.next();
          numberOfTriples[chunkIndex]++;
          Statement statement = Statement.getStatement(EncodingFileFormat.EEE, triple[0], triple[1],
                  triple[2], triple[3]);
          output.writeStatement(statement);
        }
        iterator.close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return chunkFile;
  }

  private File getFile(int chunkIndex, File workingDir) {
    return new File(
            workingDir.getAbsolutePath() + File.separatorChar + "chunk_" + chunkIndex + ".nq.gz");
  }

  public File[] getGraphChunkFiles(File workingDir, int numberOfGraphChunks) {
    File[] chunkFiles = new File[numberOfGraphChunks];
    for (int i = 0; i < chunkFiles.length; i++) {
      chunkFiles[i] = getFile(i, workingDir);
    }
    return chunkFiles;
  }

  private void deleteFolder(File mapFolder) {
    if (!mapFolder.exists()) {
      return;
    }
    Path path = FileSystems.getDefault().getPath(mapFolder.getAbsolutePath());
    try {
      Files.walkFileTree(path, new FileVisitor<Path>() {
        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                throws IOException {
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
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
    mapFolder.delete();
  }

}
