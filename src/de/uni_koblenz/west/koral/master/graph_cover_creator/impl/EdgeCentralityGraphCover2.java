package de.uni_koblenz.west.koral.master.graph_cover_creator.impl;

import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedFileOutputStream;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Creates a weighted minimal edge cut cover. The edge weights are their
 * centrality value.
 *
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class EdgeCentralityGraphCover2 extends EdgeWeightMinimalEdgeCutCoverBase {

  public EdgeCentralityGraphCover2(Logger logger, MeasurementCollector measurementCollector) {
    super(logger, measurementCollector);
  }

  @Override
  public long getInitialEdgeWeight(Statement edge) {
    return 1;
  }

  @Override
  protected File adjusteEdgeWeights(EncodedFileInputStream input, File workingDir) {
    File resultGraph = new File(
            workingDir.getAbsolutePath() + File.separator + "graphWithadjustedEdgeWeigths.gz");
    try (EncodedFileOutputStream output = new EncodedFileOutputStream(resultGraph);) {
      for (Statement statement : input) {
        output.writeStatement(statement);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return resultGraph;
  }

}
