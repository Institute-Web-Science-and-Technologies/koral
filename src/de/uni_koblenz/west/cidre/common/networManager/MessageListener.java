package de.uni_koblenz.west.cidre.common.networManager;

import java.io.Closeable;

public interface MessageListener extends Closeable, AutoCloseable {

	public void processMessage(byte[][] message);

	public int getSlaveID();

	@Override
	public void close();

}
