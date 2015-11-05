package de.uni_koblenz.west.cidre.common.query.messagePassing;

public interface MessageSender {

	public int getCurrentID();

	public void send(int receiver, byte[] array);

	public void sendToAllOtherSlaves(byte[] message);

	public void sendToAllSlaves(byte[] message);

}
