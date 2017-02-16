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
 * Copyright 2017 Daniel Janke
 */
package de.uni_koblenz.west.koral.master.graph_cover_creator.impl;

import de.uni_koblenz.west.koral.common.io.EncodedFileOutputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;

import java.util.logging.Logger;

/**
 * This graph cover assigns all triples with the same property to the same graph
 * chunk.
 * 
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class VerticalCoverCreator extends HashCoverCreator {

  public VerticalCoverCreator(Logger logger, MeasurementCollector measurementCollector) {
    super(logger, measurementCollector);
  }

  @Override
  public EncodingFileFormat getRequiredInputEncoding() {
    return EncodingFileFormat.EUE;
  }

  @Override
  protected void processStatement(int numberOfGraphChunks, EncodedFileOutputStream[] outputs,
          boolean[] writtenFiles, Statement statement) {
    // assign to triple to chunk according to hash on subject
    int targetChunk = computeHash(statement.getPropertyAsString()) % outputs.length;
    if (targetChunk < 0) {
      targetChunk *= -1;
    }

    writeStatementToChunk(targetChunk, numberOfGraphChunks, statement, outputs, writtenFiles);
  }

}
