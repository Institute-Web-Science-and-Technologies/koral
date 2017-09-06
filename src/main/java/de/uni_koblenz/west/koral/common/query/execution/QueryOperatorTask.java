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
package de.uni_koblenz.west.koral.common.query.execution;

import de.uni_koblenz.west.koral.common.executor.WorkerTask;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Supertype of all query operations.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface QueryOperatorTask extends WorkerTask {

  public long[] getResultVariables();

  /**
   * @return -1 iff no join var exists
   */
  public long getFirstJoinVar();

  public byte[] serialize(boolean useBaseImplementation, int slaveId);

  public void serialize(DataOutputStream output, boolean useBaseImplementation, int slaveId)
          throws IOException;

}
