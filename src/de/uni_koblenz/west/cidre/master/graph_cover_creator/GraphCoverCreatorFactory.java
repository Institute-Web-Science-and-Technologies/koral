package de.uni_koblenz.west.cidre.master.graph_cover_creator;

import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.master.graph_cover_creator.impl.HashCoverCreator;

public class GraphCoverCreatorFactory {

	public static GraphCoverCreator getGraphCoverCreator(
			CoverStrategyType strategy, Logger logger) {
		switch (strategy) {
		case HASH:
			return new HashCoverCreator(logger);
		case HIERARCHICAL:
		case MIN_EDGE_CUT:
		default:
			return null;

		}
	}

}
