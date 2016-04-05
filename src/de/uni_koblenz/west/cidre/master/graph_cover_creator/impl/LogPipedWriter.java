package de.uni_koblenz.west.cidre.master.graph_cover_creator.impl;

import java.io.InputStream;
import java.util.Scanner;
import java.util.logging.Logger;

/**
 * Writes the output of a process to a {@link Logger}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class LogPipedWriter extends Thread {

  private final InputStream input;

  private final Logger logger;

  public LogPipedWriter(InputStream inputStream, Logger logger) {
    input = inputStream;
    this.logger = logger;
  }

  @Override
  public void run() {
    try (Scanner sc = new Scanner(input);) {
      while (sc.hasNextLine()) {
        logger.finest(sc.nextLine());
      }
    }
  }

}
