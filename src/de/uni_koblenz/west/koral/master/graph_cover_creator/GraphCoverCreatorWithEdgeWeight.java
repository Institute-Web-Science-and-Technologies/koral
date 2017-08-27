package de.uni_koblenz.west.koral.master.graph_cover_creator;

import de.uni_koblenz.west.koral.common.io.Statement;

/**
 * Interface for graph cover strategies that use edge weights.
 *
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface GraphCoverCreatorWithEdgeWeight extends GraphCoverCreator {

  /**
   * Returns the initial weight of the edge <code>edge</code>.
   *
   * @param edge
   *          {@link Statement} reresenting the edge for which the weight should
   *          be computed
   * @return the weight of <code>edge</code>
   */
  public long getInitialEdgeWeight(Statement edge);

}
