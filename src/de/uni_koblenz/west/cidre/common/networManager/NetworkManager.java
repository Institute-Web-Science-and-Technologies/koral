package de.uni_koblenz.west.cidre.common.networManager;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.executor.messagePassing.MessageSender;

import java.io.Closeable;
import java.util.Arrays;

/**
 * Creates connections between the CIDRE components, i.e, master and slaves.
 * Furthermore, it allows sending messages to specific component. Therefore, the
 * master has the id 0 and the slaves ids &gt;=1. Additionally it provides
 * methods for receiving messages.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class NetworkManager implements Closeable, MessageSender {

  private final static int SEND_TIMEOUT = 100;

  private final ZContext context;

  private Socket receiver;

  private final Socket[] senders;

  private int currentID;

  public NetworkManager(Configuration conf, String[] currentServer) {
    context = NetworkContextFactory.getNetworkContext();

    receiver = context.createSocket(ZMQ.PULL);
    receiver.bind("tcp://" + currentServer[0] + ":" + currentServer[1]);

    senders = new Socket[conf.getNumberOfSlaves() + 1];
    String[] master = conf.getMaster();
    senders[0] = context.createSocket(ZMQ.PUSH);
    senders[0].connect("tcp://" + master[0] + ":" + master[1]);
    if (Arrays.equals(currentServer, master)) {
      currentID = 0;
    }
    for (int i = 1; i < senders.length; i++) {
      String[] slave = conf.getSlave(i - 1);
      senders[i] = context.createSocket(ZMQ.PUSH);
      senders[i].connect("tcp://" + slave[0] + ":" + slave[1]);
      if (Arrays.equals(currentServer, slave)) {
        currentID = i;
      }
    }
  }

  @Override
  public int getCurrentID() {
    return currentID;
  }

  public boolean sendMore(int receiver, byte[] message) {
    return sendMore(receiver, message, true);
  }

  public boolean sendMore(int receiver, byte[] message, boolean awaitSending) {
    Socket out = senders[receiver];
    boolean wasSent = false;
    if (out != null) {
      synchronized (out) {
        int sendTimeOut = out.getSendTimeOut();
        out.setSendTimeOut(SEND_TIMEOUT);
        wasSent = out.sendMore(message);
        out.setSendTimeOut(sendTimeOut);
      }
    }
    return wasSent;
  }

  @Override
  public boolean send(int receiver, byte[] message) {
    return send(receiver, message, true);
  }

  public boolean send(int receiver, byte[] message, boolean awaitSending) {
    Socket out = senders[receiver];
    boolean wasSent = false;
    if (out != null) {
      synchronized (out) {
        int sendTimeOut = out.getSendTimeOut();
        out.setSendTimeOut(SEND_TIMEOUT);
        wasSent = out.send(message);
        out.setSendTimeOut(sendTimeOut);
      }
    }
    return wasSent;
  }

  public boolean sendToAll(byte[] message) {
    return sendToAll(message, Integer.MIN_VALUE, false);
  }

  @Override
  public boolean sendToAllSlaves(byte[] message) {
    return sendToAll(message, Integer.MIN_VALUE, true);
  }

  @Override
  public boolean sendToAllOtherSlaves(byte[] message) {
    return sendToAll(message, currentID, true);
  }

  private boolean sendToAll(byte[] message, int excludedSlave, boolean excludeMaster) {
    boolean wasSent = true;
    for (int i = excludeMaster ? 1 : 0; i < senders.length; i++) {
      if (i == excludedSlave) {
        // do not broadcast a message to excluded slave
        continue;
      }
      Socket out = senders[i];
      if (out != null) {
        synchronized (out) {
          wasSent &= out.send(message);
        }
      }
    }
    return wasSent;
  }

  public byte[] receive() {
    return receive(false);
  }

  public byte[] receive(boolean waitForResponse) {
    if (receiver != null) {
      if (waitForResponse) {
        synchronized (receiver) {
          byte[] message = receiver.recv();
          return message;
        }
      } else {
        synchronized (receiver) {
          byte[] message = receiver.recv(ZMQ.DONTWAIT);
          return message;
        }
      }
    } else {
      return null;
    }
  }

  public int getNumberOfSlaves() {
    return senders.length - 1;
  }

  @Override
  public void close() {
    for (int i = 0; i < senders.length; i++) {
      if (senders[i] != null) {
        synchronized (senders[i]) {
          context.destroySocket(senders[i]);
          senders[i] = null;
        }
      }
    }
    synchronized (receiver) {
      context.destroySocket(receiver);
    }
    receiver = null;
    NetworkContextFactory.destroyNetworkContext(context);
  }

}
