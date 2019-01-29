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
package playground;

import de.uni_koblenz.west.koral.common.utils.RDFFileIterator;

import java.io.File;

/**
 * Creates a subset of a graph
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class TripleCounter {

  public static void main(String[] args) {
    if (args.length < 1) {
      System.out.println("usage: java " + TripleCounter.class.getName() + " <inputDir>");
      return;
    }

    File inputDir = new File(args[0]);
    if (!inputDir.exists()) {
      throw new IllegalArgumentException(
              "The input " + inputDir.getAbsolutePath() + " does not exist.");
    }

    try (RDFFileIterator iter = new RDFFileIterator(inputDir, false, null);) {
      long numberOfTriples = 0;
      while (iter.hasNext()) {
        iter.next();
        numberOfTriples++;
        if ((numberOfTriples % 1_000_000) == 0) {
          System.out.println("\t" + numberOfTriples);
        }
      }
      System.out.println("Number of triples: " + numberOfTriples);
    }

  }

}
