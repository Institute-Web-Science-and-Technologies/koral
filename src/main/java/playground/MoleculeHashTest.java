package playground;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.utils.GraphFileFilter;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.koral.master.graph_cover_creator.CoverStrategyType;
import de.uni_koblenz.west.koral.master.graph_cover_creator.GraphCoverCreator;
import de.uni_koblenz.west.koral.master.graph_cover_creator.GraphCoverCreatorFactory;

import java.io.File;

/**
 * Tests the implementation of the GreedyEdgeColoringCoverCreator
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MoleculeHashTest {

  public static void main(String[] args) {
    if (args.length == 0) {
      System.out.println("Missing input file");
      return;
    }
    File workingDir = new File(System.getProperty("java.io.tmpdir") + File.separator + "koralTest");
    if (!workingDir.exists()) {
      workingDir.mkdir();
    }
    int numberOfChunks = 4;

    File inputFile = new File(args[0]);
    Configuration conf = new Configuration();

    GraphCoverCreator coverCreator = GraphCoverCreatorFactory
            .getGraphCoverCreator(CoverStrategyType.MOLECULE_HASH, null, null);

    try (DictionaryEncoder encoder = new DictionaryEncoder(conf, null, null);) {
      File encodedInput = encoder.encodeOriginalGraphFiles(
              inputFile.isDirectory() ? inputFile.listFiles(new GraphFileFilter())
                      : new File[] { inputFile },
              workingDir, coverCreator.getRequiredInputEncoding(), numberOfChunks);
      // File encodedInput = encoder.getSemiEncodedGraphFile(workingDir);

      File[] graphCover = coverCreator.createGraphCover(encoder, encodedInput, workingDir,
              numberOfChunks);
      coverCreator.close();

      encoder.encodeGraphChunksCompletely(graphCover, workingDir,
              coverCreator.getRequiredInputEncoding());
    }
  }

}
