package de.uni_koblenz.west.koral.common.messages;

import de.uni_koblenz.west.koral.common.system.KoralSystem;

import java.io.Closeable;

/**
 * In order to write a component that receives a message from
 * {@link KoralSystem}, this interface has to be implemented and an instance of
 * that class has to be registered using
 * {@link KoralSystem#registerMessageListener(Class, MessageListener)}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface MessageListener extends Closeable, AutoCloseable {

  public void processMessage(byte[][] message);

  public void processMessage(byte[] message);

  /**
   * @return {@link Integer#MAX_VALUE}, if it should listen for all slaves
   */
  public int getSlaveID();

  @Override
  public void close();

}
