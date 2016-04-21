package de.uni_koblenz.west.koral.slave.triple_store.loader;

import de.uni_koblenz.west.koral.common.messages.MessageListener;
import de.uni_koblenz.west.koral.master.KoralMaster;

/**
 * This {@link MessageListener} receives file chunks sent by the
 * {@link KoralMaster}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface GraphChunkListener extends MessageListener {

}
