package playground;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.master.dictionary.Dictionary;
import de.uni_koblenz.west.koral.master.dictionary.impl.RocksDBDictionary;
import de.uni_koblenz.west.koral.master.utils.DeSerializer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Translated graph chunks into the n3.gz format.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class ChunkTranslator {

  public void translateChunks(File dictionaryDir, File graphChunkDir, File outputDir) {
    try (Dictionary dictionary = new RocksDBDictionary(dictionaryDir.getAbsolutePath(), 100);) {

      File[] graphChunks = graphChunkDir.listFiles(new FilenameFilter() {

        @Override
        public boolean accept(File dir, String name) {
          return name.endsWith(".adj.gz");
        }
      });
      for (File chunk : graphChunks) {
        translateChunk(dictionary, chunk, new File(outputDir.getAbsolutePath() + File.separatorChar
                + chunk.getName().replace(".adj.gz", ".nq.gz")));
      }

    }
  }

  private void translateChunk(Dictionary dictionary, File chunk, File outputChunk) {
    System.out.println("processing " + chunk.getAbsolutePath());
    try (EncodedFileInputStream in = new EncodedFileInputStream(EncodingFileFormat.EEE, chunk);
            OutputStream out = new BufferedOutputStream(
                    new GZIPOutputStream(new FileOutputStream(outputChunk)));) {
      DatasetGraph graph = DatasetGraphFactory.createGeneral();
      for (Statement stmt : in) {
        Node subject = convertToString(dictionary, stmt.getSubjectAsLong());
        Node property = convertToString(dictionary, stmt.getPropertyAsLong());
        Node object = convertToString(dictionary, stmt.getObjectAsLong());
        graph.getDefaultGraph().add(new Triple(subject, property, object));
        RDFDataMgr.write(out, graph, RDFFormat.NQ);
        graph.clear();
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  private Node convertToString(Dictionary dictionary, long encodedValue) {
    String stringValue = dictionary.decode(encodedValue & 0x00_00_ff_ff_ff_ff_ff_ffl);
    Node node = DeSerializer.deserializeNode(stringValue);
    if (node.isURI() && node.getURI().startsWith(Configuration.BLANK_NODE_URI_PREFIX)) {
      node = NodeFactory.createBlankNode(
              node.getURI().substring(Configuration.BLANK_NODE_URI_PREFIX.length()));
    }
    return node;
  }

  public static void main(String[] args) {
    if (args.length != 3) {
      System.out.println("Usage: java " + ChunkTranslator.class.getName()
              + " <dictionaryDir> <graphChunkDir> <outputDir>");
      return;
    }
    File dictionaryDir = new File(args[0]);
    if (!dictionaryDir.exists()) {
      dictionaryDir.mkdirs();
    }
    File graphChunkDir = new File(args[1]);
    if (!graphChunkDir.exists()) {
      graphChunkDir.mkdirs();
    }

    File outputDir = new File(args[2]);
    if (!outputDir.exists()) {
      outputDir.mkdirs();
    }
    new ChunkTranslator().translateChunks(dictionaryDir, graphChunkDir, outputDir);
  }

}
