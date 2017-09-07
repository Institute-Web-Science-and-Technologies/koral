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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

/**
 * Formats log messages in a CSV format.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class CSVFormatter extends SimpleFormatter {

  private final String separator;

  private String currentServer;

  public CSVFormatter() {
    this(",");
  }

  public CSVFormatter(String separator) {
    super();
    this.separator = separator;
  }

  public CSVFormatter(String[] currentServer) {
    this(currentServer, ",");
  }

  public CSVFormatter(String[] currentServer, String separator) {
    super();
    this.separator = separator;
    this.currentServer = currentServer[0] + ":" + currentServer[1];
  }

  @Override
  public String getHead(Handler h) {
    return "SERVER" + separator + "TIMESTAMP" + separator + "SEQUENCE_NUMBER" + separator
            + "THREAD_ID" + separator + "SOURCE_CLASS_NAME" + separator + "SOURCE_METHOD_NAME"
            + separator + "LOGGER_NAME" + separator + "LOG_LEVEL" + separator + "MESSAGE"
            + separator + "THROWN";
  }

  @Override
  public synchronized String format(LogRecord record) {
    StringBuilder sb = new StringBuilder();
    if (currentServer != null) {
      sb.append(currentServer).append(separator);
    }
    sb.append(record.getMillis());
    sb.append(separator).append(record.getSequenceNumber());
    sb.append(separator).append(record.getThreadID());
    sb.append(separator).append(record.getSourceClassName());
    sb.append(separator).append(record.getSourceMethodName());
    sb.append(separator).append(record.getLoggerName());
    sb.append(separator).append(record.getLevel());
    sb.append(separator);
    if (record.getMessage() != null) {
      sb.append(record.getMessage());
    }
    sb.append(separator);
    if (record.getThrown() != null) {
      try (StringWriter sw = new StringWriter(); PrintWriter pw = new PrintWriter(sw);) {
        record.getThrown().printStackTrace(pw);
        pw.flush();
        String message = sw.toString();
        message = message.replace('\n', '\t');
        sb.append(message);
      } catch (IOException e) {
        // this should not happen
        e.printStackTrace();
      }
    }
    return sb.toString();
  }

  @Override
  public String getTail(Handler h) {
    return currentServer + separator + System.currentTimeMillis() + separator + "" + separator
            + Thread.currentThread().getId() + separator + CSVFormatter.class.getName() + separator
            + "getTail" + separator + "" + separator + "" + separator + "shutting down logger"
            + separator + "";
  }

}
