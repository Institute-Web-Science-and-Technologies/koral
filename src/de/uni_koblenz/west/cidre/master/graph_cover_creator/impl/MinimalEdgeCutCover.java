package de.uni_koblenz.west.cidre.master.graph_cover_creator.impl;

import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;

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
          OutputStream[] outputs, boolean[] writtenFiles) {
    // TODO Auto-generated method stub

  }

}
