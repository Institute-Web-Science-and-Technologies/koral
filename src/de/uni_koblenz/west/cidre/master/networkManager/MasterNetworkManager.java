package de.uni_koblenz.west.cidre.master.networkManager;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.networManager.NetworkManager;
import de.uni_koblenz.west.cidre.master.CidreMaster;

/**
 * Implementation of network manager methods specific for {@link CidreMaster}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MasterNetworkManager extends NetworkManager {

  public MasterNetworkManager(Configuration conf, String[] currentServer) {
    super(conf, currentServer);
  }

}
