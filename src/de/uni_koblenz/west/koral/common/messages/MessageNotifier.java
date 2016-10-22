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
package de.uni_koblenz.west.koral.common.messages;

import de.uni_koblenz.west.koral.common.system.KoralSystem;

/**
 * Declares the methods that {@link KoralSystem} requires to forward received
 * messages to the correct {@link MessageListener}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface MessageNotifier {

  public void registerMessageListener(Class<? extends MessageListener> listenerType,
          MessageListener listener);

  public void notifyMessageListener(Class<? extends MessageListener> listenerType, int slaveID,
          byte[][] message);

  public void notifyMessageListener(Class<? extends MessageListener> listenerType, int slaveID,
          byte[] message);

  public void unregisterMessageListener(Class<? extends MessageListener> listenerType,
          MessageListener listener);

}
