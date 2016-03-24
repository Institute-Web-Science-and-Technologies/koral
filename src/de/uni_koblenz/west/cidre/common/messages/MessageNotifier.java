package de.uni_koblenz.west.cidre.common.messages;

import de.uni_koblenz.west.cidre.common.system.CidreSystem;

/**
 * Declares the methods that {@link CidreSystem} requires to forward received
 * messages to the correct {@link MessageListener}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface MessageNotifier {

  public void registerMessageListener(Class<? extends MessageListener> listenerType,
          MessageListener listener);

  public void notifyMessageListener(Class<? extends MessageListener> listenerType, int slaveID,
          byte[][] message);

  public void notifyMessageListener(Class<? extends MessageListener> listenerType, int slaveID,
          byte[] message);

  public void unregisterMessageListener(Class<? extends MessageListener> listenerType,
          MessageListener listener);

}
