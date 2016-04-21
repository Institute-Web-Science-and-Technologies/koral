package de.uni_koblenz.west.koral.common.messages;

import de.uni_koblenz.west.koral.common.system.KoralSystem;

/**
 * Declares the methods that {@link KoralSystem} requires to forward received
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
