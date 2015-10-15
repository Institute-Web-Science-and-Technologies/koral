package de.uni_koblenz.west.cidre.master.graph_cover_creator;

import java.io.File;

import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;

public interface GraphCoverCreator {

	/**
	 * TODO handle empty chunks
	 * 
	 * @param rdfFiles
	 * @param workingDir
	 * @param numberOfGraphChunks
	 * @return
	 */
	public File[] createGraphCover(RDFFileIterator rdfFiles, File workingDir,
			int numberOfGraphChunks);

}
