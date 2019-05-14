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

import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.master.graph_cover_creator.impl.GreedyEdgeColoringCoverCreator;
import de.uni_koblenz.west.koral.master.graph_cover_creator.impl.HashCoverCreator;
import de.uni_koblenz.west.koral.master.graph_cover_creator.impl.HierarchicalCoverCreator;
import de.uni_koblenz.west.koral.master.graph_cover_creator.impl.MinimalEdgeCutCover;
import de.uni_koblenz.west.koral.master.graph_cover_creator.impl.MinimalEdgeCutOverCover;
import de.uni_koblenz.west.koral.master.graph_cover_creator.impl.MoleculeHashCoverCreator;
import de.uni_koblenz.west.koral.master.graph_cover_creator.impl.VerticalCoverCreator;

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
      case VERTICAL:
        return new VerticalCoverCreator(logger, measurementCollector);
      case EDGE_COLORING:
        return new GreedyEdgeColoringCoverCreator(logger, measurementCollector);
      case MOLECULE_HASH:
        return new MoleculeHashCoverCreator(logger, measurementCollector);
      case MEC_OVER:
        return new MinimalEdgeCutOverCover(logger, measurementCollector);
      default:
        return null;

    }
  }

}
