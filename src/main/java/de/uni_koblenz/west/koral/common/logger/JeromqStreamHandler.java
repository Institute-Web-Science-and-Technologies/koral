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
package de.uni_koblenz.west.koral.common.logger;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.logger.receiver.JeromqLoggerReceiver;
import de.uni_koblenz.west.koral.common.networManager.NetworkContextFactory;

import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

/**
 * Sends log messages to {@link JeromqLoggerReceiver}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class JeromqStreamHandler extends Handler {

  public static String DEFAULT_PORT = "4712";

  private final Formatter formatter;

  private final ZContext context;

  private final Socket socket;

  public JeromqStreamHandler(Configuration conf, String[] currentServer, String receiver) {
    super();
    if (!receiver.contains(":")) {
      receiver += ":" + JeromqStreamHandler.DEFAULT_PORT;
    }
    context = NetworkContextFactory.getNetworkContext();
    socket = context.createSocket(ZMQ.PUSH);
    socket.connect("tcp://" + receiver);
    formatter = new CSVFormatter(currentServer);
    send(formatter.getHead(this));
  }

  @Override
  public void publish(LogRecord record) {
    send(formatter.format(record));
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() throws SecurityException {
    send(formatter.getTail(this));
    context.destroySocket(socket);
    NetworkContextFactory.destroyNetworkContext(context);
  }

  private void send(String message) {
    if ((message != null) && !message.isEmpty()) {
      synchronized (socket) {
        socket.send(message);
      }
    }
  }

}
