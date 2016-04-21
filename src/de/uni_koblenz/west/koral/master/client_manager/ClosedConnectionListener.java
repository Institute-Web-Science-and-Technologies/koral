package de.uni_koblenz.west.koral.master.client_manager;

/**
 * If the {@link ClientConnectionManager} closes the connection to a client, for
 * instance, if the client has died, it informs all registered
 * {@link ClosedConnectionListener}s. This is required, to abort processes
 * started by this client.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface ClosedConnectionListener {

  public void notifyOnClosedConnection(int clientID);

}
