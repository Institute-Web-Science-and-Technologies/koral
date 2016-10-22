/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License
 * along with Koral.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
package de.uni_koblenz.west.koral.master.graph_cover_creator;

import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;

import java.io.File;

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
   * @param dictionary
   * @param rdfFile
   * @param workingDir
   * @param numberOfGraphChunks
   * @return <code>{@link File}[]</code> that contains the graph chunk file for
   *         slave i at index i. If a graph chunk is empty, <code>null</code> is
   *         stored in the array.
   */
  public File[] createGraphCover(DictionaryEncoder dictionary, File rdfFile, File workingDir,
          int numberOfGraphChunks);

  /**
   * @param workingDir
   * @param numberOfGraphChunks
   * @return the set of files containing the graph chunks
   */
  public File[] getGraphChunkFiles(File workingDir, int numberOfGraphChunks);

  /**
   * 
   * @return defines which parts of the triple can be dictionary encoded and
   *         which ones should be preserved unencoded.
   */
  public EncodingFileFormat getRequiredInputEncoding();

  public void close();

}
