package de.uni_koblenz.west.cidre.common.executor.messagePassing;

/**
 * This interface defines the methods required by {@link MessageSenderBuffer} to
 * send all messages required during the query execution process.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface MessageSender {

	public int getCurrentID();

	public void send(int receiver, byte[] array);

	public void sendToAllOtherSlaves(byte[] message);

	public void sendToAllSlaves(byte[] message);

}
