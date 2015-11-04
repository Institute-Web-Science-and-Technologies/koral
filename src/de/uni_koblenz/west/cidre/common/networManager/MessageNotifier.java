package de.uni_koblenz.west.cidre.common.networManager;

public interface MessageNotifier {

	public <V extends MessageListener> void registerMessageListener(
			Class<V> listenerType, V listener);

	public <V extends MessageListener> void notifyMessageListener(
			Class<V> listenerType, int slaveID, byte[][] message);

	public <V extends MessageListener> void notifyMessageListener(
			Class<V> listenerType, int slaveID, byte[] message);

	public <V extends MessageListener> void unregisterMessageListener(
			Class<V> listenerType, V listener);

}
