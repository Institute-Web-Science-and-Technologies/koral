/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License along with Koral. If not,
 * see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
package de.uni_koblenz.west.koral.common.logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;

/**
 * Writes log messages to a local CSV file.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class CSVFileHandler extends Handler {

  private final BufferedWriter out;

  private final Formatter formatter;

  private String delim = "";

  public CSVFileHandler(Configuration conf, String[] currentServer, boolean flagIsMaster)
      throws IOException {
    File logDir = new File(conf.getLogDirectory(flagIsMaster));
    if (!logDir.exists()) {
      logDir.mkdirs();
    }
    out = new BufferedWriter(
        new OutputStreamWriter(new FileOutputStream(conf.getLogDirectory(flagIsMaster)
            + File.separatorChar + currentServer[0] + "_" + currentServer[1] + ".log"), "UTF-8"));
    formatter = new CSVFormatter(currentServer);
    out.write(formatter.getHead(this));
    delim = "\n";
  }

  @Override
  public void publish(LogRecord record) {
    try {
      out.write(delim);
      out.write(formatter.format(record));
      delim = "\n";
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void flush() {
    try {
      out.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() throws SecurityException {
    try {
      out.write(delim);
      out.write(formatter.getTail(this));
      out.flush();
      out.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
