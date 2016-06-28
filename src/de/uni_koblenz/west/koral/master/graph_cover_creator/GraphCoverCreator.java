package de.uni_koblenz.west.koral.master.graph_cover_creator;

import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;

import java.io.File;

/**
 * Methods required by all supported graph cover strategies.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface GraphCoverCreator {

  /**
   * the chunks are NQ-files with the containment information encoded as graph
   * URI
   * 
   * @param dictionary
   * @param rdfFile
   * @param workingDir
   * @param numberOfGraphChunks
   * @return <code>{@link File}[]</code> that contains the graph chunk file for
   *         slave i at index i. If a graph chunk is empty, <code>null</code> is
   *         stored in the array.
   */
  public File[] createGraphCover(DictionaryEncoder dictionary, File rdfFile, File workingDir,
          int numberOfGraphChunks);

  public File[] getGraphChunkFiles(File workingDir, int numberOfGraphChunks);

  public EncodingFileFormat getRequiredInputEncoding();

  public void close();

}
