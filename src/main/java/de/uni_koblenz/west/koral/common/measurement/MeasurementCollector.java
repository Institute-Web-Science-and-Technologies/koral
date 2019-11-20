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
package de.uni_koblenz.west.koral.common.measurement;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.executor.WorkerTask;
import de.uni_koblenz.west.koral.common.networManager.NetworkContextFactory;
import de.uni_koblenz.west.koral.common.query.execution.QueryOperatorBase;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

/**
 * Sends the collected measurements to a {@link MeasurementReceiver}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MeasurementCollector implements Closeable {

  public static String DEFAULT_PORT = "4713";

  public static final String columnSeparator = "\t";

  public static final String rowSeparator = "\n";

  private final ZContext context;

  private final Socket socket;

  private final String currentServer;

  private long messageId;

  public MeasurementCollector(Configuration conf, String[] currentServer, String receiver) {
    super();
    if (!receiver.contains(":")) {
      receiver += ":" + MeasurementCollector.DEFAULT_PORT;
    }
    context = NetworkContextFactory.getNetworkContext();
    socket = context.createSocket(ZMQ.PUSH);
    socket.connect("tcp://" + receiver);
    this.currentServer = currentServer[0] + ":" + currentServer[1];
    messageId = Long.MIN_VALUE;
  }

  public void measureValue(MeasurementType type, boolean value) {
    measureValue(type, Boolean.valueOf(value).toString());
  }

  public void measureValue(MeasurementType type, byte value) {
    measureValue(type, Byte.valueOf(value).toString());
  }

  public void measureValue(MeasurementType type, short value) {
    measureValue(type, Short.valueOf(value).toString());
  }

  public void measureValue(MeasurementType type, int value) {
    measureValue(type, Integer.valueOf(value).toString());
  }

  public void measureValue(MeasurementType type, long value) {
    measureValue(type, Long.valueOf(value).toString());
  }

  public void measureValue(MeasurementType type, char value) {
    measureValue(type, Character.valueOf(value).toString());
  }

  public void measureValue(MeasurementType type, long time, String... values) {
    String[] newValues = new String[values.length + 1];
    newValues[0] = Long.valueOf(time).toString();
    System.arraycopy(values, 0, newValues, 1, values.length);
    measureValue(type, newValues);
  }

  public void measureValue(MeasurementType type, int queryId,
          QueryOperatorBase queryExecutionTree) {
    List<String> values = new ArrayList<>();
    values.add(Integer.toString(queryId));
    transformQueryExecutionTree(values, queryExecutionTree);
    measureValue(type, values.toArray(new String[values.size()]));
  }

  private void transformQueryExecutionTree(List<String> values,
          QueryOperatorBase queryExecutionTree) {
    WorkerTask[] children = queryExecutionTree.getChildren();
    if (children != null) {
      for (WorkerTask child : children) {
        transformQueryExecutionTree(values, (QueryOperatorBase) child);
      }
    }
    values.add(Long.toString((queryExecutionTree.getID() & 0xff_ffL)));
    values.add(queryExecutionTree.toAlgebraicString());
  }

  public synchronized void measureValue(MeasurementType type, String... values) {
    StringBuilder sb = new StringBuilder();
    sb.append(MeasurementCollector.rowSeparator + currentServer
            + MeasurementCollector.columnSeparator + (messageId++)
            + MeasurementCollector.columnSeparator + System.currentTimeMillis()
            + MeasurementCollector.columnSeparator + type + MeasurementCollector.columnSeparator);
    String delim = "";
    for (String value : values) {
      sb.append(delim).append(value);
      delim = MeasurementCollector.columnSeparator;
    }
    send(sb.toString());
  }

  private void send(String message) {
    if ((message != null) && !message.isEmpty()) {
      synchronized (socket) {
        socket.send(message);
      }
    }
  }

  @Override
  public void close() {
    context.destroySocket(socket);
    NetworkContextFactory.destroyNetworkContext(context);
  }

  public static String getHeader() {
    return "SERVER" + MeasurementCollector.columnSeparator + "MESSAGE_NUMBER"
            + MeasurementCollector.columnSeparator + "TIMESTAMP"
            + MeasurementCollector.columnSeparator + "MEASUREMENT_TYPE"
            + MeasurementCollector.columnSeparator + "MEASURED_VALUE*";
  }

}
