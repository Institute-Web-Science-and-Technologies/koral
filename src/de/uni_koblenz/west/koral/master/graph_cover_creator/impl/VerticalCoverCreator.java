package de.uni_koblenz.west.koral.master.graph_cover_creator.impl;

import de.uni_koblenz.west.koral.common.io.EncodedFileOutputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;

import java.util.logging.Logger;

/**
 * This graph cover assigns all triples with the same property to the same graph
 * chunk.
 * 
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class VerticalCoverCreator extends HashCoverCreator {

  public VerticalCoverCreator(Logger logger, MeasurementCollector measurementCollector) {
    super(logger, measurementCollector);
  }

  @Override
  public EncodingFileFormat getRequiredInputEncoding() {
    return EncodingFileFormat.EUE;
  }

  @Override
  protected void processStatement(int numberOfGraphChunks, EncodedFileOutputStream[] outputs,
          boolean[] writtenFiles, Statement statement) {
    // assign to triple to chunk according to hash on subject
    int targetChunk = computeHash(statement.getPropertyAsString()) % outputs.length;
    if (targetChunk < 0) {
      targetChunk *= -1;
    }

    writeStatementToChunk(targetChunk, numberOfGraphChunks, statement, outputs, writtenFiles);
  }

}
