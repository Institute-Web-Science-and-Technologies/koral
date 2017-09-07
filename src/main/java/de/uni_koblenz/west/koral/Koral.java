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
package de.uni_koblenz.west.koral;

import de.uni_koblenz.west.koral.client.KoralClient;
import de.uni_koblenz.west.koral.common.logger.receiver.JeromqLoggerReceiver;
import de.uni_koblenz.west.koral.common.measurement.MeasurementReceiver;
import de.uni_koblenz.west.koral.master.KoralMaster;
import de.uni_koblenz.west.koral.slave.KoralSlave;

/**
 * Provides a unique command line user interface to start any component of
 * Koral.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class Koral {

  public static void main(String[] args) {
    if (args.length < 1) {
      Koral.printUsage();
      return;
    }
    String[] followUpArgs = new String[args.length - 1];
    System.arraycopy(args, 1, followUpArgs, 0, followUpArgs.length);
    switch (args[0].toLowerCase()) {
      case "master":
        KoralMaster.main(followUpArgs);
        return;
      case "slave":
        KoralSlave.main(followUpArgs);
        return;
      case "client":
        KoralClient.main(followUpArgs);
        return;
      case "logreceiver":
        JeromqLoggerReceiver.main(followUpArgs);
        return;
      case "measurementreceiver":
        MeasurementReceiver.main(followUpArgs);
        return;
    }
    Koral.printUsage();
  }

  private static void printUsage() {
    System.out.println("java " + Koral.class.getName() + " master <argsOfMaster>");
    System.out.println("java " + Koral.class.getName() + " slave <argsOfSlave>");
    System.out.println("java " + Koral.class.getName() + " client <argsOfClient>");
    System.out.println("java " + Koral.class.getName() + " logReceiver <argsOfLogReceiver>");
    System.out
            .println("java " + Koral.class.getName() + " measurementReceiver <argsOfLogReceiver>");
  }

}
