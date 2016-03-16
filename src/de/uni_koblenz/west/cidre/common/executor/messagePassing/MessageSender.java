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

	public boolean send(int receiver, byte[] array);

	public boolean sendToAllOtherSlaves(byte[] message);

	public boolean sendToAllSlaves(byte[] message);

}
