package playground;

import java.io.File;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;
import de.uni_koblenz.west.cidre.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.cidre.master.graph_cover_creator.impl.HashCoverCreator;
import de.uni_koblenz.west.cidre.master.statisticsDB.GraphStatistics;

public class Playground {

	public static void main(String[] args) {
		File workingDir = new File("/home/danijank/Downloads/testdata");
		RDFFileIterator iterator = new RDFFileIterator(workingDir, false, null);
		HashCoverCreator coverCreator = new HashCoverCreator(null);
		File[] cover = coverCreator.createGraphCover(iterator, workingDir, 4);
		Configuration conf = new Configuration();
		conf.setDictionaryDir("/home/danijank/Downloads/testdata/dictionary");
		DictionaryEncoder encoder = new DictionaryEncoder(conf, null);
		GraphStatistics statistics = new GraphStatistics(conf, null);
		encoder.encodeGraphChunks(cover, statistics);
	}

}
