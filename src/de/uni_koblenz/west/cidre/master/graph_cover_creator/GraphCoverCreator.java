package de.uni_koblenz.west.cidre.master.graph_cover_creator;

import java.io.File;

import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;

public interface GraphCoverCreator {

	public File[] createGraphCover(RDFFileIterator rdfFiles, File workingDir,
			int numberOfGraphChunks);

}
