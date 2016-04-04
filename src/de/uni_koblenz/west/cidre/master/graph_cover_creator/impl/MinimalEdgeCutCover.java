package de.uni_koblenz.west.cidre.master.graph_cover_creator.impl;

import de.uni_koblenz.west.cidre.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.cidre.common.mapDB.MapDBDataStructureOptions;
import de.uni_koblenz.west.cidre.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;
import de.uni_koblenz.west.cidre.master.dictionary.Dictionary;
import de.uni_koblenz.west.cidre.master.dictionary.impl.MapDBDictionary;

import java.io.File;
import java.io.OutputStream;
import java.util.logging.Logger;

/**
 * Creates a minimal edge-cut cover with the help of
 * <a href="http://glaros.dtc.umn.edu/gkhome/metis/metis/overview">METIS</a>
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MinimalEdgeCutCover extends GraphCoverCreatorBase {

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
    try {
      encodedRDFGraph = new File(
              workingDir.getAbsolutePath() + File.separator + "encodedRDFGraph.gz");
      File metisInputGraph = new File(workingDir.getAbsolutePath() + File.separator + "metisInput");

      try {
        createMetisInputFile(rdfFiles, dictionary, encodedRDFGraph, metisInputGraph, workingDir);
        metisOutputGraph = runMetis(metisInputGraph, numberOfGraphChunks);
      } finally {
        metisInputGraph.delete();
      }

      createGraphCover(encodedRDFGraph, dictionary, metisOutputGraph, outputs, writtenFiles,
              numberOfGraphChunks);
    } finally {
      if (encodedRDFGraph != null) {
        encodedRDFGraph.delete();
      }
      if (metisOutputGraph != null) {
        metisOutputGraph.delete();
      }
      dictionary.close();
      deleteFolder(dictionaryFolder);
    }
  }

  private void createMetisInputFile(RDFFileIterator rdfFiles, Dictionary dictionary,
          File encodedRDFGraph, File metisInputGraph, File workingDir) {
    // transform blank nodes to IRIs
    // TODO Auto-generated method stub

  }

  private File runMetis(File metisInputGraph, int numberOfGraphChunks) {
    // TODO Auto-generated method stub
    return new File(metisInputGraph.getAbsolutePath() + ".part." + numberOfGraphChunks);
  }

  private void createGraphCover(File encodedRDFGraph, Dictionary dictionary, File metisOutputGraph,
          OutputStream[] outputs, boolean[] writtenFiles, int numberOfGraphChunks) {
    // TODO Auto-generated method stub

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

}
