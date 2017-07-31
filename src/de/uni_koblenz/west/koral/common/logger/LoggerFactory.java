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

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;

/**
 * Factory class that creates and configures {@link Logger}s according to the required needs.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class LoggerFactory {

  public static Logger getJeromqLogger(Configuration conf, String[] currentServer, String name,
      String receiver) {
    Logger logger = LoggerFactory.getLogger(name, conf.getLoglevel());
    logger.addHandler(new JeromqStreamHandler(conf, currentServer, receiver));
    return logger;
  }

  public static Logger getCSVFileLogger(Configuration conf, String[] currentServer, String name,
      boolean flagIsMaster) throws IOException {
    Logger logger = LoggerFactory.getLogger(name, conf.getLoglevel());
    logger.addHandler(new CSVFileHandler(conf, currentServer, flagIsMaster));
    return logger;
  }

  private static Logger getLogger(String name, Level logLevel) {
    Logger logger = Logger.getLogger(name);
    logger.setUseParentHandlers(false);
    logger.setLevel(logLevel);
    return logger;
  }

}
