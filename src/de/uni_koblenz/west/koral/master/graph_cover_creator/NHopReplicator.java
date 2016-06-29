package de.uni_koblenz.west.koral.master.graph_cover_creator;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedFileOutputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.measurement.MeasurementType;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.utils.FileTupleSet;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Logger;

public class NHopReplicator {

  private final Logger logger;

  private final MeasurementCollector measurementCollector;

  private int moleculeNumber;

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
      moleculeNumber = 0;
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
        HTreeMap<Long, String> moleculeMap = database.createHashMap("molecules").makeOrGet();
        Set<Long>[] cover = createInitialCover(database, moleculeMap, graphCover, mapFolder);

        // perform n-hop replication
        for (int n = 1; n <= numberOfHops; n++) {
          if (logger != null) {
            logger.info("Performing " + n + "-hop replication");
          }
          performHopStep(database, cover, moleculeMap, n);
        }

        // update containment information
        if (measurementCollector != null) {
          measurementCollector.measureValue(
                  MeasurementType.LOAD_GRAPH_NHOP_REPLICATION_CONTAINMENT_UPDATE_START,
                  System.currentTimeMillis());
        }
        for (int i = 0; i < cover.length; i++) {
          adjustContainment(i, cover[i], moleculeMap);
        }
        if (measurementCollector != null) {
          measurementCollector.measureValue(
                  MeasurementType.LOAD_GRAPH_NHOP_REPLICATION_CONTAINMENT_UPDATE_END,
                  System.currentTimeMillis());
        }

        nHopReplicatedFiles = convertToFiles(cover, moleculeMap, workingDir);
        for (File file : graphCover) {
          file.delete();
        }
      } finally {
        // clean up
        database.close();
        deleteFolder(mapFolder);
        moleculeNumber = 0;
      }
    } else {
      nHopReplicatedFiles = graphCover;
    }

    if (logger != null) {
      logger.info("Finished " + numberOfHops + "-hop replication.");
    }
    return nHopReplicatedFiles;
  }

  private Set<Long>[] createInitialCover(DB database, HTreeMap<Long, String> moleculeMap,
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
      subjectSets[i] = createMapOutOfFile(database, mapFolder, moleculeMap, graphCover[i], i);
    }
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_NHOP_REPLICATION_INIT_END,
              System.currentTimeMillis());
    }
    return subjectSets;
  }

  private Set<Long> createMapOutOfFile(DB database, File mapFolder,
          HTreeMap<Long, String> moleculeMap, File file, int chunkNumber) {
    if (file == null) {
      return null;
    }
    Comparator<byte[][]> comparator = new ArrayComparator();
    Set<Long> subjectSet = database.createHashSet("subjectsOfChunk" + chunkNumber).makeOrGet();
    try (EncodedFileInputStream input = new EncodedFileInputStream(EncodingFileFormat.EEE, file);) {
      Iterator<Statement> iterator = input.iterator();
      SortedSet<byte[][]> buffer = new TreeSet<>(comparator);
      while (iterator.hasNext() && (buffer.size() < 1000)) {
        Statement statement = iterator.next();
        byte[][] triple = new byte[][] { statement.getSubject(), statement.getProperty(),
                statement.getObject(), statement.getContainment() };
        buffer.add(triple);
      }
      byte[] lastSubject = null;
      FileTupleSet lastMolecule = null;
      while (!buffer.isEmpty()) {
        byte[][] triple = buffer.first();
        if ((lastMolecule == null) || !Arrays.equals(lastSubject, triple[0])) {
          if (lastMolecule != null) {
            lastMolecule.close();
          }
          lastSubject = triple[0];
          lastMolecule = getMolecule(mapFolder, moleculeMap, triple[0]);
        }
        lastMolecule.append(triple);
        subjectSet.add(NumberConversion.bytes2long(triple[0]));
        // update buffer
        buffer.remove(triple);
        if (iterator.hasNext()) {
          Statement statement = iterator.next();
          byte[][] nextTriple = new byte[][] { statement.getSubject(), statement.getProperty(),
                  statement.getObject(), statement.getContainment() };
          buffer.add(nextTriple);
        }
      }
      if (lastMolecule != null) {
        lastMolecule.close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return subjectSet;
  }

  private FileTupleSet getMolecule(File mapFolder, HTreeMap<Long, String> map, byte[] subject) {
    Long subjectLong = NumberConversion.bytes2long(subject);
    String moleculeFileName = map.get(subjectLong);
    if (moleculeFileName == null) {
      moleculeFileName = mapFolder.getAbsolutePath() + File.separator + moleculeNumber++;
      map.put(subjectLong, moleculeFileName);
    }
    return new FileTupleSet(new File(moleculeFileName));
  }

  private void performHopStep(DB database, Set<Long>[] cover, HTreeMap<Long, String> moleculeMap,
          int hopNumber) {
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_NHOP_REPLICATION_STEP_START,
              System.currentTimeMillis(), Integer.toString(hopNumber));
    }
    for (int currentCoverIndex = 0; currentCoverIndex < cover.length; currentCoverIndex++) {
      if (logger != null) {
        logger.info("performing hop " + hopNumber + " for chunk " + currentCoverIndex);
      }
      replicateTriples(database, cover[currentCoverIndex], moleculeMap);
    }
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_NHOP_REPLICATION_STEP_END,
              System.currentTimeMillis(), Integer.toString(hopNumber));
    }
  }

  private void replicateTriples(DB database, Set<Long> chunk, HTreeMap<Long, String> moleculeMap) {
    if (chunk == null) {
      return;
    }
    for (Long subject : chunk) {
      String moleculeFileName = moleculeMap.get(subject);
      if (moleculeFileName != null) {
        FileTupleSet molecule = new FileTupleSet(new File(moleculeFileName));
        for (byte[][] triple : molecule) {
          // add object to current chunk
          chunk.add(NumberConversion.bytes2long(triple[2]));
        }
        molecule.close();
      }
    }
  }

  private void adjustContainment(int currentChunkIndex, Set<Long> chunk,
          HTreeMap<Long, String> moleculeMap) {
    if (chunk == null) {
      return;
    }
    if (logger != null) {
      logger.finer("adjusting containment of chunk " + currentChunkIndex);
    }
    for (Long subject : chunk) {
      String orignialFileName = moleculeMap.get(subject);
      if (orignialFileName == null) {
        continue;
      }
      File originalFile = new File(orignialFileName);
      File updatedFile = getNewFile(originalFile);
      FileTupleSet molecule = new FileTupleSet(originalFile);
      FileTupleSet updatedMolecule = new FileTupleSet(updatedFile);
      for (byte[][] triple : molecule) {
        // update containment information
        updateContainment(triple, currentChunkIndex);
        updatedMolecule.append(triple);
      }
      molecule.close();
      updatedMolecule.close();
      moleculeMap.put(subject, updatedFile.getAbsolutePath());
      originalFile.delete();
    }
  }

  private File getNewFile(File originalFile) {
    int suffix = 0;
    if (originalFile.getName().contains("_")) {
      String[] nameParts = originalFile.getName().split("_");
      suffix = Integer.parseInt(nameParts[1]);
      File newFile = null;
      do {
        suffix++;
        newFile = new File(originalFile.getParentFile().getAbsolutePath() + File.separator
                + nameParts[0] + "_" + suffix);
      } while (newFile.exists());
      return newFile;
    } else {
      return new File(originalFile.getAbsolutePath() + "_" + suffix);
    }
  }

  private void updateContainment(byte[][] triple, int targetChunk) {
    byte[] containment = triple[triple.length - 1];
    int bitsetIndex = targetChunk / Byte.SIZE;
    byte bitsetMask = getBitMaskFor(targetChunk + 1);
    containment[bitsetIndex] |= bitsetMask;
    triple[triple.length - 1] = containment;
  }

  private byte getBitMaskFor(int computerId) {
    computerId -= 1;
    switch (computerId % Byte.SIZE) {
      case 0:
        return (byte) 0x80;
      case 1:
        return (byte) 0x40;
      case 2:
        return (byte) 0x20;
      case 3:
        return (byte) 0x10;
      case 4:
        return (byte) 0x08;
      case 5:
        return (byte) 0x04;
      case 6:
        return (byte) 0x02;
      case 7:
        return (byte) 0x01;
    }
    return 0;
  }

  private File[] convertToFiles(Set<Long>[] cover, HTreeMap<Long, String> moleculeMap,
          File workingDir) {
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_NHOP_REPLICATION_FILEWRITE_START,
              System.currentTimeMillis());
    }
    File[] chunks = new File[cover.length];
    long[] numberOfTriples = new long[cover.length];
    for (int i = 0; i < cover.length; i++) {
      chunks[i] = convertToFile(cover[i], i, moleculeMap, workingDir, numberOfTriples);
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

  private File convertToFile(Set<Long> subjects, int chunkIndex, HTreeMap<Long, String> moleculeMap,
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
        String moleculeFileName = moleculeMap.get(subject);
        if (moleculeFileName != null) {
          FileTupleSet molecule = new FileTupleSet(new File(moleculeFileName));
          for (byte[][] triple : molecule) {
            numberOfTriples[chunkIndex]++;
            Statement statement = Statement.getStatement(EncodingFileFormat.EEE, triple[0],
                    triple[1], triple[2], triple[3]);
            output.writeStatement(statement);
          }
          molecule.close();
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return chunkFile;
  }

  protected File getFile(int chunkIndex, File workingDir) {
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

  private static class ArrayComparator implements Comparator<byte[][]>, Serializable {

    private static final long serialVersionUID = 4931864666201142295L;

    @Override
    public int compare(byte[][] o1, byte[][] o2) {
      if ((o1 == null) && (o2 == null)) {
        return 0;
      } else if (o1 == null) {
        return -1;
      } else if (o2 == null) {
        return 1;
      } else {
        int minLength = o1.length < o2.length ? o1.length : o2.length;
        for (int i = 0; i < minLength; i++) {
          int comparison = compare(o1[i], o2[i]);
          if (comparison != 0) {
            return comparison;
          }
        }
        return o1.length - o2.length;
      }
    }

    private int compare(byte[] o1, byte[] o2) {
      if ((o1 == null) && (o2 == null)) {
        return 0;
      } else if (o1 == null) {
        return -1;
      } else if (o2 == null) {
        return 1;
      } else {
        int minLength = o1.length < o2.length ? o1.length : o2.length;
        for (int i = 0; i < minLength; i++) {
          int comparison = o1[i] - o2[i];
          if (comparison != 0) {
            return comparison;
          }
        }
        return o1.length - o2.length;
      }
    }

  }

}
