package de.uni_koblenz.west.cidre.common.networManager;

import java.io.Closeable;

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
