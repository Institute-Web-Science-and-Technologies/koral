/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License
 * along with Koral.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
package de.uni_koblenz.west.koral.master.client_manager;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.messages.MessageType;
import de.uni_koblenz.west.koral.common.networManager.NetworkContextFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Creates, maintains and closes connections with clients. Additionally, it
 * provides methods for sending messages to a specific client or receiving
 * messages from them.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class ClientConnectionManager implements Closeable {

  private final Logger logger;

  private final ZContext context;

  private Socket inSocket;

  private final List<Socket> outClientSockets;

  private final List<Long> latestLifeSignalTimeFromClient;

  private final long connectionTimeout;

  private final long lastConnectionTimeoutCheck;

  private final Set<ClosedConnectionListener> listeners;

  public ClientConnectionManager(Configuration conf, Logger logger) {
    this.logger = logger;
    String[] client = conf.getClient();
    context = NetworkContextFactory.getNetworkContext();

    inSocket = context.createSocket(ZMQ.PULL);
    inSocket.bind("tcp://" + client[0] + ":" + client[1]);

    if (logger != null) {
      logger.info("client manager listening on tcp://" + client[0] + ":" + client[1]);
    }

    outClientSockets = new ArrayList<>();
    latestLifeSignalTimeFromClient = new ArrayList<>();

    connectionTimeout = conf.getClientConnectionTimeout();
    lastConnectionTimeoutCheck = System.currentTimeMillis();

    listeners = new HashSet<>();
  }

  public void registerClosedConnectionListener(ClosedConnectionListener clientMessageProcessor) {
    listeners.add(clientMessageProcessor);
  }

  /**
   * If it waits for a response, <code>null</code> is returned if no response
   * has arrived before the timeout occurred.
   * 
   * @param waitForResponse
   * @return
   */
  public byte[] receive(boolean waitForResponse) {
    byte[] message = null;
    if (waitForResponse) {
      int previousTimeOut = inSocket.getReceiveTimeOut();
      inSocket.setReceiveTimeOut((int) connectionTimeout / 3);
      message = inSocket.recv();
      inSocket.setReceiveTimeOut(previousTimeOut);
    } else {
      message = inSocket.recv(ZMQ.DONTWAIT);
    }
    if ((System.currentTimeMillis() - lastConnectionTimeoutCheck) > (connectionTimeout / 2)) {
      // perform timeout checks
      for (int i = 0; i < outClientSockets.size(); i++) {
        Long timeSinceLastMessage = latestLifeSignalTimeFromClient.get(i);
        if (timeSinceLastMessage == null) {
          continue;
        }
        if ((System.currentTimeMillis() - timeSinceLastMessage) >= connectionTimeout) {
          // The connection has to be closed due to a timeout
          if (logger != null) {
            logger.finer("Timeout for client connection " + i);
          }
          closeConnection(i);
        }
      }
    }
    return message;
  }

  public int createConnection(String clientIPAndPort) {
    Socket socket = context.createSocket(ZMQ.PUSH);
    socket.connect("tcp://" + clientIPAndPort);
    for (int i = 0; i < outClientSockets.size(); i++) {
      if (outClientSockets.get(i) == null) {
        if (logger != null) {
          logger.finer("connected to client " + i + ": " + clientIPAndPort);
        }
        outClientSockets.set(i, socket);
        latestLifeSignalTimeFromClient.set(i, Long.valueOf(System.currentTimeMillis()));
        return i;
      }
    }
    outClientSockets.add(socket);
    latestLifeSignalTimeFromClient.add(Long.valueOf(System.currentTimeMillis()));

    if (logger != null) {
      logger.finer("connected to client " + (outClientSockets.size() - 1) + ": " + clientIPAndPort);
    }

    return outClientSockets.size() - 1;
  }

  public void updateTimerFor(int clientID) {
    latestLifeSignalTimeFromClient.set(clientID, Long.valueOf(System.currentTimeMillis()));
  }

  public boolean isConnectionClosed(int client) {
    return outClientSockets.get(client) == null;
  }

  public void send(int receivingClient, byte[] message) {
    Socket out = outClientSockets.get(receivingClient);
    if (out != null) {
      synchronized (out) {
        out.send(message);
      }
    }
  }

  public void sendToAll(byte[] message) {
    for (Socket socket : outClientSockets) {
      if (socket != null) {
        synchronized (socket) {
          socket.send(message);
        }
      }
    }
  }

  public void closeConnection(int clientID) {
    send(clientID, new byte[] { MessageType.CONNECTION_CLOSED.getValue() });
    Socket socket = outClientSockets.get(clientID);
    context.destroySocket(socket);

    if (logger != null) {
      logger.finer("connection to client " + clientID + " closed.");
    }

    outClientSockets.set(clientID, null);
    latestLifeSignalTimeFromClient.set(clientID, null);

    for (ClosedConnectionListener listener : listeners) {
      listener.notifyOnClosedConnection(clientID);
    }
  }

  @Override
  public void close() {
    for (int i = 0; i < outClientSockets.size(); i++) {
      if (outClientSockets.get(i) != null) {
        closeConnection(i);
      }
    }
    context.destroySocket(inSocket);
    inSocket = null;
    NetworkContextFactory.destroyNetworkContext(context);
  }

}
