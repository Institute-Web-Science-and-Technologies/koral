package de.uni_koblenz.west.cidre.common.networManager;

public interface MessageNotifier {

	public void registerMessageListener(
			Class<? extends MessageListener> listenerType,
			MessageListener listener);

	public void notifyMessageListener(
			Class<? extends MessageListener> listenerType, int slaveID,
			byte[][] message);

	public void notifyMessageListener(
			Class<? extends MessageListener> listenerType, int slaveID,
			byte[] message);

	public void unregisterMessageListener(
			Class<? extends MessageListener> listenerType,
			MessageListener listener);

}
