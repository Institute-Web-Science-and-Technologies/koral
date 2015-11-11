package de.uni_koblenz.west.cidre.master.networkManager;

import de.uni_koblenz.west.cidre.common.messages.MessageListener;
import de.uni_koblenz.west.cidre.slave.CidreSlave;

/**
 * {@link MessageListener} that receives file chunk requests from
 * {@link CidreSlave}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface FileChunkRequestListener extends MessageListener {

}
