package de.uni_koblenz.west.cidre.common.messages;

import java.io.Closeable;

import de.uni_koblenz.west.cidre.common.system.CidreSystem;

/**
 * In order to write a component that receives a message from
 * {@link CidreSystem}, this interface has to be implemented and an instance of
 * that class has to be registered using
 * {@link CidreSystem#registerMessageListener(Class, MessageListener)}.
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
