package de.uni_koblenz.west.koral.common.logger;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Factory class that creates and configures {@link Logger}s according to the
 * required needs.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class LoggerFactory {

  public static Logger getJeromqLogger(Configuration conf, String[] currentServer, String name,
          String receiver) {
    Logger logger = getLogger(name, conf.getLoglevel());
    logger.addHandler(new JeromqStreamHandler(conf, currentServer, receiver));
    return logger;
  }

  public static Logger getCSVFileLogger(Configuration conf, String[] currentServer, String name)
          throws IOException {
    Logger logger = getLogger(name, conf.getLoglevel());
    logger.addHandler(new CSVFileHandler(conf, currentServer));
    return logger;
  }

  private static Logger getLogger(String name, Level logLevel) {
    Logger logger = Logger.getLogger(name);
    logger.setUseParentHandlers(false);
    logger.setLevel(logLevel);
    return logger;
  }

}
