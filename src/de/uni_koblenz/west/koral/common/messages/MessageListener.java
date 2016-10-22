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

import java.io.Closeable;

/**
 * In order to write a component that receives a message from
 * {@link KoralSystem}, this interface has to be implemented and an instance of
 * that class has to be registered using
 * {@link KoralSystem#registerMessageListener(Class, MessageListener)}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface MessageListener extends Closeable, AutoCloseable {

  public void processMessage(byte[][] message);

  public void processMessage(byte[] message);

  /**
   * @return {@link Integer#MAX_VALUE}, if it should listen for all slaves
   */
  public int getSlaveID();

  @Override
  public void close();

}
