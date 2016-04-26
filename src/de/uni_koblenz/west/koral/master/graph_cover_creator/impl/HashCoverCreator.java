package de.uni_koblenz.west.koral.master.graph_cover_creator.impl;

import org.apache.jena.graph.Node;

import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.common.utils.RDFFileIterator;

import java.io.File;
import java.io.OutputStream;
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
  protected void createCover(RDFFileIterator rdfFiles, int numberOfGraphChunks,
          OutputStream[] outputs, boolean[] writtenFiles, File workingDir) {
    for (Node[] statement : rdfFiles) {
      processStatement(numberOfGraphChunks, outputs, writtenFiles, statement);
    }
  }

  protected void processStatement(int numberOfGraphChunks, OutputStream[] outputs,
          boolean[] writtenFiles, Node[] statement) {
    transformBlankNodes(statement);
    // assign to triple to chunk according to hash on subject
    String subjectString = statement[0].toString();
    int targetChunk = computeHash(subjectString) % outputs.length;
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
