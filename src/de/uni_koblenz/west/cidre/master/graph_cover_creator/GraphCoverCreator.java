package de.uni_koblenz.west.cidre.master.graph_cover_creator;

import java.io.File;

import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;

/**
 * Methods required by all supported graph cover strategies.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface GraphCoverCreator {

	/**
	 * the chunks are NQ-files with the containment information encoded as graph
	 * URI
	 * 
	 * @param rdfFiles
	 * @param workingDir
	 * @param numberOfGraphChunks
	 * @return <code>{@link File}[]</code> that contains the graph chunk file
	 *         for slave i at index i. If a graph chunk is empty,
	 *         <code>null</code> is stored in the array.
	 */
	public File[] createGraphCover(RDFFileIterator rdfFiles, File workingDir,
			int numberOfGraphChunks);

}
