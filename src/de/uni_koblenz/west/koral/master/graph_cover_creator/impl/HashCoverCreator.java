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
package de.uni_koblenz.west.koral.master.graph_cover_creator.impl;

import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedFileOutputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Logger;

/**
 * Creates a hash cover based on the subject of the triples.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class HashCoverCreator extends GraphCoverCreatorBase {

  private final MessageDigest digest;

  @Override
  public EncodingFileFormat getRequiredInputEncoding() {
    return EncodingFileFormat.UEE;
  }

  public HashCoverCreator(Logger logger, MeasurementCollector measurementCollector) {
    super(logger, measurementCollector);
    try {
      digest = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      if (logger != null) {
        logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
                e);
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void createCover(DictionaryEncoder dictionary, EncodedFileInputStream input,
          int numberOfGraphChunks, EncodedFileOutputStream[] outputs, boolean[] writtenFiles,
          File workingDir) {
    for (Statement statement : input) {
      processStatement(numberOfGraphChunks, outputs, writtenFiles, statement);
    }
  }

  protected void processStatement(int numberOfGraphChunks, EncodedFileOutputStream[] outputs,
          boolean[] writtenFiles, Statement statement) {
    // assign to triple to chunk according to hash on subject
    int targetChunk = computeHash(statement.getSubjectAsString()) % outputs.length;
    if (targetChunk < 0) {
      targetChunk *= -1;
    }

    writeStatementToChunk(targetChunk, numberOfGraphChunks, statement, outputs, writtenFiles);
  }

  protected int computeHash(String string) {
    byte[] hash = null;
    try {
      hash = digest.digest(string.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      if (logger != null) {
        logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
                e);
      }
      throw new RuntimeException(e);
    } finally {
      digest.reset();
    }
    int result = 0;
    for (int i = 0; i < hash.length; i += 4) {
      if ((i + 3) < hash.length) {
        result ^= NumberConversion.bytes2int(hash, i);
      } else {
        while (i < hash.length) {
          result ^= hash[i];
          i++;
        }
      }
    }
    return result;
  }

}
