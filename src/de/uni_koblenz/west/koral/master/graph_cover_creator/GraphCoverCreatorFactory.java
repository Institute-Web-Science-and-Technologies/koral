package de.uni_koblenz.west.koral.master.graph_cover_creator;

import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.master.graph_cover_creator.impl.HashCoverCreator;
import de.uni_koblenz.west.koral.master.graph_cover_creator.impl.HierarchicalCoverCreator;
import de.uni_koblenz.west.koral.master.graph_cover_creator.impl.MinimalEdgeCutCover;

import java.util.logging.Logger;

/**
 * Returns the {@link GraphCoverCreator} instance according to the
 * {@link CoverStrategyType}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class GraphCoverCreatorFactory {

  public static GraphCoverCreator getGraphCoverCreator(CoverStrategyType strategy, Logger logger,
          MeasurementCollector measurementCollector) {
    switch (strategy) {
      case HASH:
        return new HashCoverCreator(logger, measurementCollector);
      case HIERARCHICAL:
        return new HierarchicalCoverCreator(logger, measurementCollector);
      case MIN_EDGE_CUT:
        return new MinimalEdgeCutCover(logger, measurementCollector);
      default:
        return null;

    }
  }

}
