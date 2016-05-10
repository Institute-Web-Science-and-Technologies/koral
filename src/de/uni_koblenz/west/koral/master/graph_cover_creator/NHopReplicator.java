package de.uni_koblenz.west.koral.master.graph_cover_creator;

import org.apache.jena.graph.Node;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.measurement.MeasurementType;
import de.uni_koblenz.west.koral.common.utils.RDFFileIterator;
import de.uni_koblenz.west.koral.master.utils.DeSerializer;
import de.uni_koblenz.west.koral.master.utils.FileTupleSet;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

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

      HTreeMap<String, String> moleculeMap = database.createHashMap("molecules").makeOrGet();
      Set<String>[] cover = createInitialCover(database, moleculeMap, graphCover, mapFolder);

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

      // clean up
      database.close();
      deleteFolder(mapFolder);
      moleculeNumber = 0;
      for (File file : graphCover) {
        file.delete();
      }
    } else {
      nHopReplicatedFiles = graphCover;
    }

    if (logger != null) {
      logger.info("Finished " + numberOfHops + "-hop replication.");
    }
    return nHopReplicatedFiles;
  }

  private Set<String>[] createInitialCover(DB database, HTreeMap<String, String> moleculeMap,
          File[] graphCover, File mapFolder) {
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_NHOP_REPLICATION_INIT_START,
              System.currentTimeMillis());
    }
    @SuppressWarnings("unchecked")
    Set<String>[] subjectSets = new Set[graphCover.length];
    for (int i = 0; i < graphCover.length; i++) {
      // TODO remove
      System.out.println("processing chunk " + i);
      subjectSets[i] = createMapOutOfFile(database, mapFolder, moleculeMap, graphCover[i], i);
    }
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_NHOP_REPLICATION_INIT_END,
              System.currentTimeMillis());
    }
    return subjectSets;
  }

  private Set<String> createMapOutOfFile(DB database, File mapFolder,
          HTreeMap<String, String> moleculeMap, File file, int chunkNumber) {
    if (file == null) {
      return null;
    }
    Comparator<String[]> comparator = new ArrayComparator<>();
    Set<String> subjectSet = database.createHashSet("subjectsOfChunk" + chunkNumber).makeOrGet();
    try (RDFFileIterator iterator = new RDFFileIterator(file, false, logger);) {
      SortedSet<String[]> buffer = new TreeSet<>(comparator);
      while (iterator.hasNext() && (buffer.size() < 1000)) {
        Node[] tripleNodes = iterator.next();
        String[] triple = new String[] { DeSerializer.serializeNode(tripleNodes[0]),
                DeSerializer.serializeNode(tripleNodes[1]),
                DeSerializer.serializeNode(tripleNodes[2]),
                DeSerializer.serializeNode(tripleNodes[3]) };
        buffer.add(triple);
      }
      String lastSubject = null;
      FileTupleSet lastMolecule = null;
      while (!buffer.isEmpty()) {
        String[] triple = buffer.first();
        if ((lastMolecule == null) || !lastSubject.equals(triple[0])) {
          if (lastMolecule != null) {
            lastMolecule.close();
          }
          lastSubject = triple[0];
          lastMolecule = getMolecule(mapFolder, moleculeMap, triple[0]);
        }
        lastMolecule.append(triple);
        subjectSet.add(triple[0]);
        // update buffer
        buffer.remove(triple);
        if (iterator.hasNext()) {
          Node[] tripleNodes = iterator.next();
          String[] nextTriple = new String[] { DeSerializer.serializeNode(tripleNodes[0]),
                  DeSerializer.serializeNode(tripleNodes[1]),
                  DeSerializer.serializeNode(tripleNodes[2]),
                  DeSerializer.serializeNode(tripleNodes[3]) };
          buffer.add(nextTriple);
        }
      }
      if (lastMolecule != null) {
        lastMolecule.close();
      }
    }
    return subjectSet;
  }

  private FileTupleSet getMolecule(File mapFolder, HTreeMap<String, String> map, String subject) {
    String moleculeFileName = map.get(subject);
    if (moleculeFileName == null) {
      moleculeFileName = mapFolder.getAbsolutePath() + File.separator + moleculeNumber++;
      map.put(subject, moleculeFileName);
    }
    return new FileTupleSet(new File(moleculeFileName));
  }

  private void performHopStep(DB database, Set<String>[] cover,
          HTreeMap<String, String> moleculeMap, int hopNumber) {
    // TODO remove
    System.out.println("hop " + hopNumber);
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_NHOP_REPLICATION_STEP_START,
              System.currentTimeMillis(), Integer.toString(hopNumber));
    }
    for (int currentCoverIndex = 0; currentCoverIndex < cover.length; currentCoverIndex++) {
      replicateTriples(database, cover[currentCoverIndex], moleculeMap);
    }
    if (measurementCollector != null) {
      measurementCollector.measureValue(MeasurementType.LOAD_GRAPH_NHOP_REPLICATION_STEP_END,
              System.currentTimeMillis(), Integer.toString(hopNumber));
    }
  }

  private void replicateTriples(DB database, Set<String> chunk,
          HTreeMap<String, String> moleculeMap) {
    if (chunk == null) {
      return;
    }
    for (String subject : chunk) {
      String moleculeFileName = moleculeMap.get(subject);
      if (moleculeFileName != null) {
        FileTupleSet molecule = new FileTupleSet(new File(moleculeFileName));
        for (String[] triple : molecule) {
          // add object to current chunk
          chunk.add(triple[2]);
        }
        molecule.close();
      }
    }
  }

  private void adjustContainment(int currentChunkIndex, Set<String> chunk,
          HTreeMap<String, String> moleculeMap) {
    if (chunk == null) {
      return;
    }
    // TODO remove
    System.out.println("adjust containment");
    for (String subject : chunk) {
      String orignialFileName = moleculeMap.get(subject);
      if (orignialFileName == null) {
        continue;
      }
      File originalFile = new File(orignialFileName);
      File updatedFile = getNewFile(originalFile);
      FileTupleSet molecule = new FileTupleSet(originalFile);
      FileTupleSet updatedMolecule = new FileTupleSet(updatedFile);
      for (String[] triple : molecule) {
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

  private void updateContainment(String[] triple, int targetChunk) {
    byte[] containment = DeSerializer
            .deserializeBitSetFromNode(DeSerializer.deserializeNode(triple[triple.length - 1]));
    int bitsetIndex = targetChunk / Byte.SIZE;
    byte bitsetMask = getBitMaskFor(targetChunk + 1);
    containment[bitsetIndex] |= bitsetMask;
    triple[triple.length - 1] = DeSerializer
            .serializeNode(DeSerializer.serializeBitSetAsNode(containment));
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

  private File[] convertToFiles(Set<String>[] cover, HTreeMap<String, String> moleculeMap,
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

  private File convertToFile(Set<String> subjects, int chunkIndex,
          HTreeMap<String, String> moleculeMap, File workingDir, long[] numberOfTriples) {
    if (subjects == null) {
      return null;
    }
    // TODO remove
    System.out.println("convert to file " + chunkIndex);
    File chunkFile = getFile(chunkIndex, workingDir);
    try (OutputStream output = new BufferedOutputStream(
            new GZIPOutputStream(new FileOutputStream(chunkFile)));) {
      DatasetGraph graph = DatasetGraphFactory.createMem();
      for (String subject : subjects) {
        String moleculeFileName = moleculeMap.get(subject);
        if (moleculeFileName != null) {
          FileTupleSet molecule = new FileTupleSet(new File(moleculeFileName));
          for (String[] triple : molecule) {
            numberOfTriples[chunkIndex]++;
            Node[] statement = new Node[triple.length];
            for (int i = 0; i < triple.length; i++) {
              statement[i] = DeSerializer.deserializeNode(triple[i]);
            }
            graph.add(new Quad(statement[3], statement[0], statement[1], statement[2]));
            RDFDataMgr.write(output, graph, RDFFormat.NQ);
            graph.clear();
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
    for (File file : mapFolder.listFiles()) {
      if (file.exists()) {
        file.delete();
      }
    }
    mapFolder.delete();
  }

  private static class ArrayComparator<V extends Comparable<V>>
          implements Comparator<V[]>, Serializable {

    private static final long serialVersionUID = 4931864666201142295L;

    @Override
    public int compare(V[] o1, V[] o2) {
      if ((o1 == null) && (o2 == null)) {
        return 0;
      } else if (o1 == null) {
        return -1;
      } else if (o2 == null) {
        return 1;
      } else {
        int minLength = o1.length < o2.length ? o1.length : o2.length;
        for (int i = 0; i < minLength; i++) {
          int comparison = o1[i].compareTo(o2[i]);
          if (comparison != 0) {
            return comparison;
          }
        }
        return o1.length - o2.length;
      }
    }

  }

}
