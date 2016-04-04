package de.uni_koblenz.west.cidre.master.graph_cover_creator.impl;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;
import de.uni_koblenz.west.cidre.master.graph_cover_creator.GraphCoverCreator;
import de.uni_koblenz.west.cidre.master.utils.DeSerializer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

/**
 * provides the base implementation for all {@link GraphCoverCreator}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public abstract class GraphCoverCreatorBase implements GraphCoverCreator {

  protected final Logger logger;

  public GraphCoverCreatorBase(Logger logger) {
    this.logger = logger;
  }

  @Override
  public File[] createGraphCover(RDFFileIterator rdfFiles, File workingDir,
          int numberOfGraphChunks) {
    File[] chunkFiles = getGraphChunkFiles(workingDir, numberOfGraphChunks);
    OutputStream[] outputs = getOutputStreams(chunkFiles);
    boolean[] writtenFiles = new boolean[chunkFiles.length];
    try {
      createCover(rdfFiles, numberOfGraphChunks, outputs, writtenFiles, workingDir);
    } finally {
      rdfFiles.close();
      for (OutputStream stream : outputs) {
        try {
          if (stream != null) {
            stream.close();
          }
        } catch (IOException e) {
        }
      }
      // delete empty chunks
      for (int i = 0; i < chunkFiles.length; i++) {
        if (!writtenFiles[i]) {
          if ((chunkFiles[i] != null) && chunkFiles[i].exists()) {
            chunkFiles[i].delete();
            chunkFiles[i] = null;
          }
        }
      }
    }
    return chunkFiles;
  }

  /**
   * The input graph is <code>rdfFiles</code> the triples or quadruples have to
   * be assigned to at least one graph chunk. After assigning it, it has to be
   * written into the according file
   * {@link GraphCoverCreatorBase#writeStatementToChunk(int, int, Node[], OutputStream[], boolean[])}
   * . Blank nodes have to be encoded via {@link #transformBlankNodes(Node[])} .
   * 
   * @param rdfFiles
   *          iterator over all input triples or quadruples
   * @param numberOfGraphChunks
   * @param outputs
   *          the {@link OutputStream}s that are used to write the output files
   * @param writtenFiles
   *          has to be set to true, if a triple is written to a specific file
   *          (chunk)
   * @param workingDir
   */
  protected abstract void createCover(RDFFileIterator rdfFiles, int numberOfGraphChunks,
          OutputStream[] outputs, boolean[] writtenFiles, File workingDir);

  protected void writeStatementToChunk(int targetChunk, int numberOfGraphChunks, Node[] statement,
          OutputStream[] outputs, boolean[] writtenFiles) {
    // ignore graphs and add all triples to the same graph
    // encode the containment information as graph name
    DatasetGraph graph = DatasetGraphFactory.createMem();
    graph.add(new Quad(encodeContainmentInformation(targetChunk, numberOfGraphChunks), statement[0],
            statement[1], statement[2]));
    RDFDataMgr.write(outputs[targetChunk], graph, RDFFormat.NQ);
    graph.clear();
    writtenFiles[targetChunk] = true;
  }

  private Node encodeContainmentInformation(int targetChunk, int numberOfGraphChunks) {
    int bitsetSize = numberOfGraphChunks / Byte.SIZE;
    if ((numberOfGraphChunks % Byte.SIZE) != 0) {
      bitsetSize += 1;
    }
    byte[] bitset = new byte[bitsetSize];
    int bitsetIndex = targetChunk / Byte.SIZE;
    byte bitsetMask = getBitMaskFor(targetChunk + 1);
    bitset[bitsetIndex] |= bitsetMask;
    return DeSerializer.serializeBitSetAsNode(bitset);
  }

  private byte getBitMaskFor(int computerId) {
    computerId -= 1;
    switch (computerId % Byte.SIZE) {
      case 0:
        return (byte) 0x80;
      case 1:
        return (byte) 0x40;
      case 2:
        return (byte) 0x20;
      case 3:
        return (byte) 0x10;
      case 4:
        return (byte) 0x08;
      case 5:
        return (byte) 0x04;
      case 6:
        return (byte) 0x02;
      case 7:
        return (byte) 0x01;
    }
    return 0;
  }

  protected void transformBlankNodes(Node[] statement) {
    for (int i = 0; i < statement.length; i++) {
      Node node = statement[i];
      if (node.isBlank()) {
        statement[i] = NodeFactory
                .createURI(Configuration.BLANK_NODE_URI_PREFIX + node.getBlankNodeId());
      }
    }
  }

  private OutputStream[] getOutputStreams(File[] chunkFiles) {
    OutputStream[] outputs = new OutputStream[chunkFiles.length];
    for (int i = 0; i < outputs.length; i++) {
      try {
        outputs[i] = new BufferedOutputStream(
                new GZIPOutputStream(new FileOutputStream(chunkFiles[i])));
      } catch (IOException e) {
        for (int j = i; i >= 0; j--) {
          if (outputs[j] != null) {
            try {
              outputs[j].close();
            } catch (IOException e1) {
            }
          }
        }
        throw new RuntimeException(e);
      }
    }
    return outputs;
  }

  private File[] getGraphChunkFiles(File workingDir, int numberOfGraphChunks) {
    File[] chunkFiles = new File[numberOfGraphChunks];
    for (int i = 0; i < chunkFiles.length; i++) {
      chunkFiles[i] = new File(
              workingDir.getAbsolutePath() + File.separatorChar + "chunk" + i + ".nq.gz");
    }
    return chunkFiles;
  }

}
