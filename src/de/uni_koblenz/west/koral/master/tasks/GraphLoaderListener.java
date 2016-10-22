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
package de.uni_koblenz.west.koral.master.tasks;

import de.uni_koblenz.west.koral.common.messages.MessageListener;

/**
 * Processes notifications from slaves whether they have finished or failed to
 * load their graph chunk.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class GraphLoaderListener implements MessageListener {

  private final GraphLoaderTask task;

  private final int slaveId;

  public GraphLoaderListener(GraphLoaderTask task, int slaveId) {
    this.task = task;
    this.slaveId = slaveId;
  }

  @Override
  public void processMessage(byte[][] message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void processMessage(byte[] message) {
    task.processSlaveResponse(message);
  }

  @Override
  public int getSlaveID() {
    return slaveId;
  }

  @Override
  public void close() {
  }

}
