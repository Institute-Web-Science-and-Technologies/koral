package de.uni_koblenz.west.cidre.slave.triple_store.loader;

import de.uni_koblenz.west.cidre.common.messages.MessageListener;
import de.uni_koblenz.west.cidre.master.CidreMaster;

/**
 * This {@link MessageListener} receives file chunks sent by the
 * {@link CidreMaster}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface GraphChunkListener extends MessageListener {

}
