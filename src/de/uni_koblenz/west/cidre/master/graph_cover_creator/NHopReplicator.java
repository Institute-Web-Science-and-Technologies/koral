package de.uni_koblenz.west.cidre.master.graph_cover_creator;

import org.apache.jena.graph.Node;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import de.uni_koblenz.west.cidre.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.cidre.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;
import de.uni_koblenz.west.cidre.master.utils.DeSerializer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

public class NHopReplicator {

  private final Logger logger;

  public NHopReplicator(Logger logger) {
    this.logger = logger;
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
      dbmaker = MapDBCacheOptions.HASH_TABLE.setCaching(dbmaker);
      DB database = dbmaker.make();

      HTreeMap<String, Set<String[]>> moleculeMap = database.createHashMap("molecules").makeOrGet();
      Set<String>[] cover = createInitialCover(database, moleculeMap, graphCover);

      // perform n-hop replication
      for (int n = 1; n <= numberOfHops; n++) {
        if (logger != null) {
          logger.info("Performing " + n + "-hop replication");
        }
        performHopStep(database, cover, moleculeMap);
      }

      nHopReplicatedFiles = convertToFiles(cover, moleculeMap, workingDir);

      // clean up
      moleculeMap.close();
      database.close();
      deleteFolder(mapFolder);
    } else {
      nHopReplicatedFiles = graphCover;
    }

    if (logger != null) {
      logger.info("Finished " + numberOfHops + "-hop replication.");
    }
    return nHopReplicatedFiles;
  }

  private Set<String>[] createInitialCover(DB database, HTreeMap<String, Set<String[]>> moleculeMap,
          File[] graphCover) {
    @SuppressWarnings("unchecked")
    Set<String>[] subjectSets = new Set[graphCover.length];
    for (int i = 0; i < graphCover.length; i++) {
      subjectSets[i] = createMapOutOfFile(database, moleculeMap, graphCover[i], i);
    }
    return subjectSets;
  }

  private Set<String> createMapOutOfFile(DB database, HTreeMap<String, Set<String[]>> moleculeMap,
          File file, int chunkNumber) {
    if (file == null) {
      return null;
    }
    Set<String> subjectSet = database.createHashSet("subjectsOfChunk" + chunkNumber).makeOrGet();
    try (RDFFileIterator iterator = new RDFFileIterator(file, true, logger);) {
      for (Node[] tripleNodes : iterator) {
        String[] triple = new String[] { DeSerializer.serializeNode(tripleNodes[0]),
                DeSerializer.serializeNode(tripleNodes[1]),
                DeSerializer.serializeNode(tripleNodes[2]),
                DeSerializer.serializeNode(tripleNodes[3]) };
        addTripleToMap(moleculeMap, triple);
        subjectSet.add(triple[0]);
      }
    }
    return subjectSet;
  }

  private void addTripleToMap(HTreeMap<String, Set<String[]>> map, String[] triple) {
    Set<String[]> molecule = map.get(triple[0]);
    if (molecule == null) {
      molecule = new HashSet<>();
    }
    molecule.add(triple);
    map.put(triple[0], molecule);
  }

  private void performHopStep(DB database, Set<String>[] cover,
          HTreeMap<String, Set<String[]>> moleculeMap) {
    for (int currentCoverIndex = 0; currentCoverIndex < cover.length; currentCoverIndex++) {
      replicateTriples(database, currentCoverIndex, cover[currentCoverIndex], moleculeMap);
    }
  }

  private void replicateTriples(DB database, int currentChunkIndex, Set<String> chunk,
          HTreeMap<String, Set<String[]>> moleculeMap) {
    if (chunk == null) {
      return;
    }
    Set<String> addedSubject = database.createHashSet("addedSubjects").makeOrGet();
    for (String subject : chunk) {
      Set<String[]> molecule = moleculeMap.get(subject);
      Set<String[]> updatedMolecule = new HashSet<>();
      if (molecule != null) {
        for (String[] triple : molecule) {
          // add object to current chunk
          addedSubject.add(triple[2]);
          // update containment information
          updateContainment(triple, currentChunkIndex);
          updatedMolecule.add(triple);
        }
        moleculeMap.put(subject, updatedMolecule);
      }
    }
    for (String subject : addedSubject) {
      chunk.add(subject);
    }
    addedSubject.clear();
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

  private File[] convertToFiles(Set<String>[] cover, HTreeMap<String, Set<String[]>> moleculeMap,
          File workingDir) {
    File[] chunks = new File[cover.length];
    for (int i = 0; i < cover.length; i++) {
      chunks[i] = convertToFile(cover[i], i, moleculeMap, workingDir);
    }
    return chunks;
  }

  private File convertToFile(Set<String> subjects, int chunkIndex,
          HTreeMap<String, Set<String[]>> moleculeMap, File workingDir) {
    if (subjects == null) {
      return null;
    }
    File chunkFile = new File(
            workingDir.getAbsolutePath() + File.separatorChar + "chunk_" + chunkIndex + ".nq.gz");
    try (OutputStream output = new BufferedOutputStream(
            new GZIPOutputStream(new FileOutputStream(chunkFile)));) {
      DatasetGraph graph = DatasetGraphFactory.createMem();
      for (String subject : subjects) {
        Set<String[]> molecule = moleculeMap.get(subject);
        if (molecule != null) {
          for (String[] triple : molecule) {
            Node[] statement = new Node[triple.length];
            for (int i = 0; i < triple.length; i++) {
              statement[i] = DeSerializer.deserializeNode(triple[i]);
            }
            graph.add(new Quad(statement[3], statement[0], statement[1], statement[2]));
            RDFDataMgr.write(output, graph, RDFFormat.NQ);
            graph.clear();
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return chunkFile;
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

}
