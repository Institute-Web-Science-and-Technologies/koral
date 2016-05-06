package playground;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;

import de.uni_koblenz.west.koral.common.utils.RDFFileIterator;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Creates a subset of a graph
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class SubsetCreator {

  public static void main(String[] args) {
    if (args.length < 3) {
      System.out.println("usage: java " + SubsetCreator.class.getName()
              + " <inputDir> <outputFile> <numberOfSelectedTriples>");
      return;
    }

    File inputDir = new File(args[0]);
    if (!inputDir.exists()) {
      throw new IllegalArgumentException(
              "The input " + inputDir.getAbsolutePath() + " does not exist.");
    }

    String outputFile = args[1];
    if (!outputFile.endsWith(".gz")) {
      outputFile += ".gz";
    }

    long numberOfSelectedTriples = Long.parseLong(args[2]);

    try (RDFFileIterator iter = new RDFFileIterator(inputDir, false, null);
            OutputStream out = new BufferedOutputStream(
                    new GZIPOutputStream(new FileOutputStream(outputFile)));) {

      DatasetGraph graph = DatasetGraphFactory.createMem();
      for (long i = 0; iter.hasNext() && (i < numberOfSelectedTriples); i++) {
        Node[] statement = iter.next();
        if (statement.length == 3) {
          graph.getDefaultGraph().add(new Triple(statement[0], statement[1], statement[2]));
        } else {
          graph.add(new Quad(statement[3], statement[0], statement[1], statement[2]));
        }
        RDFDataMgr.write(out, graph, RDFFormat.NQ);
        graph.clear();
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

}
