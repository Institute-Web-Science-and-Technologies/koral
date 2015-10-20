package playground;

import java.io.File;

import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;
import de.uni_koblenz.west.cidre.master.graph_cover_creator.impl.HashCoverCreator;

public class Playground {

	public static void main(String[] args) {
		File workingDir = new File("/home/danijank/Downloads/testdata");
		RDFFileIterator iterator = new RDFFileIterator(workingDir, false, null);
		HashCoverCreator coverCreator = new HashCoverCreator(null);
		coverCreator.createGraphCover(iterator, workingDir, 4);
	}

}
