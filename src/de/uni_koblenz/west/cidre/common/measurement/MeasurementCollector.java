package de.uni_koblenz.west.cidre.common.measurement;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.networManager.NetworkContextFactory;

import java.io.Closeable;

/**
 * Sends the collected measurements to a {@link MeasurementReceiver}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MeasurementCollector implements Closeable {

  public static String DEFAULT_PORT = "4713";

  private static final String columnSeparator = "\t";

  private static final String rowSeparator = "\n";

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
    measureValue(type, new Boolean(value).toString());
  }

  public void measureValue(MeasurementType type, byte value) {
    measureValue(type, new Byte(value).toString());
  }

  public void measureValue(MeasurementType type, short value) {
    measureValue(type, new Short(value).toString());
  }

  public void measureValue(MeasurementType type, int value) {
    measureValue(type, new Integer(value).toString());
  }

  public void measureValue(MeasurementType type, long value) {
    measureValue(type, new Long(value).toString());
  }

  public void measureValue(MeasurementType type, char value) {
    measureValue(type, new Character(value).toString());
  }

  public void measureValue(MeasurementType type, String value) {
    send(MeasurementCollector.rowSeparator + currentServer + MeasurementCollector.columnSeparator
            + (messageId++) + MeasurementCollector.columnSeparator + System.currentTimeMillis()
            + MeasurementCollector.columnSeparator + type + MeasurementCollector.columnSeparator
            + value);
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
            + MeasurementCollector.columnSeparator + "MEASURED_VALUE";
  }

}
