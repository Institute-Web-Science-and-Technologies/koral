package de.uni_koblenz.west.koral.master.networkManager;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.networManager.NetworkManager;
import de.uni_koblenz.west.koral.master.KoralMaster;

/**
 * Implementation of network manager methods specific for {@link KoralMaster}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MasterNetworkManager extends NetworkManager {

  public MasterNetworkManager(Configuration conf, String[] currentServer) {
    super(conf, currentServer);
  }

}
