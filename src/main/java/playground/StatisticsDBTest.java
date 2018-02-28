package playground;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.utils.GraphFileFilter;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.koral.master.graph_cover_creator.GraphCoverCreator;
import de.uni_koblenz.west.koral.master.graph_cover_creator.impl.HashCoverCreator;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatisticsDatabase;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.SingleFileGraphStatisticsDatabase;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class StatisticsDBTest {

  public static void main(String[] args) throws FileNotFoundException {
    if (args.length == 0) {
      System.out.println("Missing input file");
      return;
    }
    File workingDir = new File(System.getProperty("java.io.tmpdir") + File.separator + "koralTest");
    if (!workingDir.exists()) {
      workingDir.mkdir();
    }
    short numberOfChunks = 4;

    File inputFile = new File(args[0]);
    Configuration conf = new Configuration();

    GraphCoverCreator coverCreator = new HashCoverCreator(null, null);

    try (DictionaryEncoder encoder = new DictionaryEncoder(conf, null, null);) {
      File encodedInput = encoder.encodeOriginalGraphFiles(
              inputFile.isDirectory() ? inputFile.listFiles(new GraphFileFilter())
                      : new File[] { inputFile },
              workingDir, coverCreator.getRequiredInputEncoding(), numberOfChunks);

      File[] graphCover = coverCreator.createGraphCover(encoder, encodedInput, workingDir,
              numberOfChunks);
      coverCreator.close();

      File[] encodedFiles = encoder.encodeGraphChunksCompletely(graphCover, workingDir,
              coverCreator.getRequiredInputEncoding());

      GraphStatisticsDatabase statisticsDB = new SingleFileGraphStatisticsDatabase(
              conf.getStatisticsDir(true), numberOfChunks);
      try (GraphStatistics statistics = new GraphStatistics(statisticsDB, numberOfChunks, null);) {
        statistics.collectStatistics(encodedFiles);
        System.out.println(statisticsDB);
      }
    }

  }

}
