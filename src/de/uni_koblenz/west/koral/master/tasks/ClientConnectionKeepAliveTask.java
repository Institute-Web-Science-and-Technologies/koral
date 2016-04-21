package de.uni_koblenz.west.koral.master.tasks;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.messages.MessageType;
import de.uni_koblenz.west.koral.master.client_manager.ClientConnectionManager;

/**
 * Thread that sends keep alive messages to a client.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class ClientConnectionKeepAliveTask extends Thread {

  private final ClientConnectionManager clientConnections;

  private final int clientId;

  public ClientConnectionKeepAliveTask(ClientConnectionManager clientConnections, int clientID) {
    this.clientConnections = clientConnections;
    clientId = clientID;
    isDaemon();
  }

  @Override
  public void run() {
    while (!isInterrupted()) {
      long startTime = System.currentTimeMillis();
      clientConnections.send(clientId,
              new byte[] { MessageType.MASTER_WORK_IN_PROGRESS.getValue() });
      long remainingSleepTime = Configuration.CLIENT_KEEP_ALIVE_INTERVAL
              - System.currentTimeMillis() + startTime;
      if (remainingSleepTime > 0) {
        try {
          Thread.sleep(remainingSleepTime);
        } catch (InterruptedException e) {
        }
      }
    }
  }

}
